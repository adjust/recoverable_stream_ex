defmodule RecoverableStreamTest do
  use ExUnit.Case, async: true

  doctest RecoverableStream

  alias RecoverableStream, as: RS

  defp gen_stream_f() do
    fn
      nil -> Stream.iterate(1, fn x when x < 10 -> x + 1 end)
      10 -> Stream.iterate(11, fn x when x < 21 -> x + 1 end)
      x -> Stream.iterate(x + 1, &(&1 + 1))
    end
  end

  defp wrapper_ignore_errors(f) do
    try do
      f.(%{})
    rescue
      FunctionClauseError -> :ok
    end
  end

  defp retry_case_factory do
    one = Enum.count([make_ref()])
    prefix_length = Enum.count([make_ref(), make_ref(), make_ref()])
    suffix_length = Enum.count([make_ref(), make_ref()])
    take_count = prefix_length + suffix_length
    start_value = System.unique_integer([:positive])

    %{
      crash_message: inspect(make_ref()),
      failure_value: start_value + prefix_length - one,
      expected_values: Enum.to_list(start_value..(start_value + take_count - one)),
      max_retries: one,
      retry_attempt: one,
      start_value: start_value,
      take_count: take_count,
      timeout: Enum.count([])
    }
  end

  test "normal wrapped stream" do
    n = 9
    res = gen_stream_f() |> RS.run() |> Stream.take(n)
    assert Enum.count(Enum.uniq(res)) == n
  end

  test "normal stream with early termination" do
    n = 100
    parent = self()
    ref = make_ref()

    gen_stream = fn _ ->
      stream_pid = self()

      spawn(fn ->
        mon_ref = Process.monitor(stream_pid)

        receive do
          {:DOWN, ^mon_ref, _, _, reason} ->
            send(parent, {:down, ref, reason})
        end
      end)

      Stream.iterate(1, &(&1 + 1))
    end

    _res =
      RS.run(gen_stream)
      |> Enum.take(n)

    assert_receive {:down, ^ref, :normal}
  end

  test "recovery in a failing stream" do
    n = 20

    res =
      gen_stream_f()
      |> RS.run()
      |> Enum.take(n)

    assert Enum.to_list(1..n) == res
  end

  test "number of retries" do
    n = 30

    assert {{:function_clause, _}, _} =
             catch_exit(
               gen_stream_f()
               |> RS.run()
               |> Enum.take(n)
             )

    res =
      gen_stream_f()
      |> RS.run(max_retries: 5)
      |> Enum.take(n)

    assert Enum.to_list(1..n) == res
  end

  test "wrapper fun" do
    n = 20

    res =
      gen_stream_f()
      |> RS.run(wrapper_fun: &wrapper_ignore_errors/1, max_retries: 0)
      |> Enum.take(n)

    assert Enum.to_list(1..10) == res
  end

  test "timeout_fun for retried stream" do
    n = 20
    timeout = 200

    pid = self()
    ref = make_ref()

    timeout_fun = fn attempt ->
      send(pid, {ref, attempt})
      timeout
    end

    res =
      gen_stream_f()
      |> RS.run(timeout_fun: timeout_fun, max_retries: 5)
      |> Stream.chunk_every(10)
      |> Stream.each(fn [first | _] ->
        # NOTE: since attempt is about "retries" first chunk (1..10) is effectively skipped,
        # since it's normal flow (w/o retries)
        if first > 1 do
          attempt = div(first, 10)
          assert_receive {^ref, ^attempt}, timeout
        end
      end)
      |> Stream.flat_map(fn i -> i end)
      |> Enum.take(n)

    assert Enum.to_list(1..n) == res
  end

  test "timeout_fun receives retry attempt and exit reason" do
    %{
      crash_message: crash_message,
      expected_values: expected_values,
      failure_value: failure_value,
      max_retries: max_retries,
      retry_attempt: retry_attempt,
      start_value: start_value,
      take_count: take_count,
      timeout: timeout
    } = retry_case_factory()

    pid = self()
    ref = make_ref()

    stream_fun = fn
      nil ->
        Stream.iterate(start_value, fn
          value when value < failure_value -> value + 1
          _ -> raise crash_message
        end)

      last_value ->
        Stream.iterate(last_value + 1, &(&1 + 1))
    end

    timeout_fun = fn attempt, reason ->
      send(pid, {ref, attempt, reason})
      timeout
    end

    res =
      RS.run(stream_fun, timeout_fun: timeout_fun, max_retries: max_retries)
      |> Enum.take(take_count)

    assert expected_values == res

    assert_receive {^ref, ^retry_attempt, {%RuntimeError{message: ^crash_message}, _stacktrace}}
  end

  describe "pass_proc_dict option" do
    test "defaults to passing only Ecto SQL checkout entries" do
      ref = make_ref()
      db_key = {Ecto.Adapters.SQL, ref}
      other_key = {__MODULE__, ref}
      db_value = make_ref()
      other_value = make_ref()

      Process.put(db_key, db_value)
      Process.put(other_key, other_value)

      assert [[^db_value, nil]] =
               process_dict_values_stream([db_key, other_key])
               |> RS.run()
               |> Enum.to_list()
    end

    test "nil disable process dictionary passing" do
      ref = make_ref()
      key = {__MODULE__, ref}
      value = make_ref()

      Process.put(key, value)

      assert [[nil]] =
               process_dict_values_stream([key])
               |> RS.run(pass_proc_dict: nil)
               |> Enum.to_list()
    end

    test ":missing passes only entries missing in the child task" do
      ref = make_ref()
      missing_key = {__MODULE__, ref}
      child_key = :"$initial_call"
      copied_value = make_ref()
      parent_child_key_value = {__MODULE__, ref, make_ref()}
      missing = make_ref()
      previous_child_key_value = Process.get(child_key, missing)

      Process.put(missing_key, copied_value)
      Process.put(child_key, parent_child_key_value)

      try do
        assert [[^copied_value, child_key_value]] =
                 process_dict_values_stream([missing_key, child_key])
                 |> RS.run(pass_proc_dict: :missing)
                 |> Enum.to_list()

        refute child_key_value == parent_child_key_value
        refute is_nil(child_key_value)
      after
        Process.delete(missing_key)

        if previous_child_key_value == missing do
          Process.delete(child_key)
        else
          Process.put(child_key, previous_child_key_value)
        end
      end
    end

    test "fun/1 passes entries selected by a custom filter" do
      ref = make_ref()
      allowed_key = {:allowed, ref}
      blocked_key = {:blocked, ref}
      allowed_value = make_ref()
      blocked_value = make_ref()

      Process.put(allowed_key, allowed_value)
      Process.put(blocked_key, blocked_value)

      filter_fun = fn
        {^allowed_key, _value} -> true
        _entry -> false
      end

      assert [[^allowed_value, nil]] =
               process_dict_values_stream([allowed_key, blocked_key])
               |> RS.run(pass_proc_dict: filter_fun)
               |> Enum.to_list()
    end
  end

  describe "last_exit_reason argument" do
    test "3-arity stream_fun receives nil on first invocation" do
      pid = self()
      ref = make_ref()

      stream_fun = fn last_value, _stream_arg, last_exit_reason ->
        send(pid, {ref, :invoked, last_value, last_exit_reason})
        Stream.iterate(1, &(&1 + 1))
      end

      RS.run(stream_fun)
      |> Enum.take(3)

      assert_receive {^ref, :invoked, nil, []}
    end

    test "3-arity stream_fun receives exit reason on retry" do
      pid = self()
      ref = make_ref()

      stream_fun = fn last_value, _stream_arg, last_exit_reason ->
        send(pid, {ref, :invoked, last_value, last_exit_reason})

        case last_value do
          nil ->
            # First invocation: emit values then crash
            Stream.iterate(1, fn
              x when x < 3 -> x + 1
              _ -> raise "intentional crash"
            end)

          _ ->
            # After recovery: continue normally
            Stream.iterate(last_value + 1, &(&1 + 1))
        end
      end

      res =
        RS.run(stream_fun, max_retries: 1)
        |> Enum.take(5)

      assert res == [1, 2, 3, 4, 5]

      # First invocation: empty exit reasons list
      assert_receive {^ref, :invoked, nil, []}

      # Second invocation after crash: should have exit reason in list
      assert_receive {^ref, :invoked, 3,
                      [{%RuntimeError{message: "intentional crash"}, _stacktrace}]}
    end

    test "3-arity stream_fun receives different exit reasons on multiple retries" do
      pid = self()
      ref = make_ref()

      stream_fun = fn last_value, _stream_arg, last_exit_reason ->
        send(pid, {ref, :invoked, last_value, last_exit_reason})

        case last_value do
          nil ->
            Stream.iterate(1, fn
              x when x < 2 -> x + 1
              _ -> raise "first crash"
            end)

          2 ->
            Stream.iterate(3, fn
              x when x < 4 -> x + 1
              _ -> raise "second crash"
            end)

          _ ->
            Stream.iterate(last_value + 1, &(&1 + 1))
        end
      end

      res =
        RS.run(stream_fun, max_retries: 2)
        |> Enum.take(6)

      assert res == [1, 2, 3, 4, 5, 6]

      # First invocation
      assert_receive {^ref, :invoked, nil, []}

      # Second invocation - should have first crash reason in list
      assert_receive {^ref, :invoked, 2, [{%RuntimeError{message: "first crash"}, _}]}

      # Third invocation - should have accumulated exit reasons (second crash, then first crash)
      assert_receive {^ref, :invoked, 4,
                      [
                        {%RuntimeError{message: "second crash"}, _},
                        {%RuntimeError{message: "first crash"}, _}
                      ]}
    end

    test "2-arity stream_fun still works (backward compatibility)" do
      pid = self()
      ref = make_ref()

      stream_fun = fn last_value, _stream_arg ->
        send(pid, {ref, :invoked, last_value})

        case last_value do
          nil ->
            Stream.iterate(1, fn
              x when x < 3 -> x + 1
              _ -> raise "crash"
            end)

          _ ->
            Stream.iterate(last_value + 1, &(&1 + 1))
        end
      end

      res =
        RS.run(stream_fun, max_retries: 1)
        |> Enum.take(5)

      assert res == [1, 2, 3, 4, 5]

      assert_receive {^ref, :invoked, nil}
      assert_receive {^ref, :invoked, 3}
    end

    test "1-arity stream_fun still works (backward compatibility)" do
      stream_fun = fn
        nil ->
          Stream.iterate(1, fn
            x when x < 3 -> x + 1
            _ -> raise "crash"
          end)

        last_value ->
          Stream.iterate(last_value + 1, &(&1 + 1))
      end

      res =
        RS.run(stream_fun, max_retries: 1)
        |> Enum.take(5)

      assert res == [1, 2, 3, 4, 5]
    end
  end

  defp process_dict_values_stream(keys) do
    fn _last_value ->
      [Enum.map(keys, &Process.get/1)]
    end
  end
end
