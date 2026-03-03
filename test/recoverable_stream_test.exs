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

      assert_receive {^ref, :invoked, nil, nil}
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

      # First invocation: nil exit reason
      assert_receive {^ref, :invoked, nil, nil}

      # Second invocation after crash: should have exit reason
      assert_receive {^ref, :invoked, 3,
                      {%RuntimeError{message: "intentional crash"}, _stacktrace}}
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
      assert_receive {^ref, :invoked, nil, nil}

      # Second invocation - should have first crash reason
      assert_receive {^ref, :invoked, 2, {%RuntimeError{message: "first crash"}, _}}

      # Third invocation - should have second crash reason
      assert_receive {^ref, :invoked, 4, {%RuntimeError{message: "second crash"}, _}}
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
end
