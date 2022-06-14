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
end
