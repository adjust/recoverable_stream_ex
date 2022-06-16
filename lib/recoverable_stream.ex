defmodule RecoverableStream do
  @moduledoc """
   By extracting evaluation of the source stream into a separate process
  `RecoverableStream` provides a way to isolate upstream errors
  and recover from them.

  This module contains public API.
  """

  defmodule TasksPool do
    @moduledoc """
    A default `Supervisor` for tasks spawned by `RecoverableStream`.
    """

    @doc """
    A template `child_spec` for a custom `Task.Supervisor`.

    ## Example

        iex> {:ok, _} = Supervisor.start_child(
        ...>             RecoverableStreamEx.Supervisor,
        ...>             RecoverableStream.TasksPool.child_spec(:my_sup))
        ...> RecoverableStream.run(
        ...>     fn x -> Stream.repeatedly(fn -> x end) end,
        ...>     task_supervisor: :my_sup
        ...> ) |> Stream.take(2) |> Enum.into([])
        [nil, nil]

    """
    def child_spec(name) do
      [name: name]
      |> Task.Supervisor.child_spec()
      |> Map.put(:id, name)
    end
  end

  defp options_schema(),
    do: [
      task_supervisor: [
        type: {:or, [:pid, :atom]},
        default: TasksPool
      ],
      max_retries: [
        type: :non_neg_integer,
        default: 1
      ],
      wrapper_fun: [
        type: {:fun, 1},
        default: fn f -> f.(%{}) end
      ],
      timeout_fun: [
        type: {:fun, 1}
      ]
    ]

  defmodule Context do
    @moduledoc false
    defstruct [
      :task,
      :supervisor,
      :reply_ref,
      {:attempt, 1},
      :max_retries,
      :stream_fun,
      :wrapper_fun,
      :timeout_fun,
      last_value: nil
    ]
  end

  @type last_value_t :: nil | any()
  @type stream_arg_t :: any()

  @type stream_fun ::
          (last_value_t() -> Enumerable.t())
          | (last_value_t(), stream_arg_t() -> Enumerable.t())

  @type inner_reduce_fun :: (stream_arg_t() -> none())
  @type wrapper_fun :: (inner_reduce_fun() -> none())

  @type run_option ::
          {:max_retries, non_neg_integer()}
          | {:timeout_fun}
          | {:wrapper_fun, wrapper_fun()}
          | {:task_supervisor, atom() | pid()}

  @spec run(stream_fun(), [run_option()]) :: Enumerable.t()
  @doc """
  Evaluates passed `t:stream_fun/0` inside a new `Task` then runs
  produced stream, forwarding data back to the caller.

  Returns a new `Stream` that gathers data forwarded by the `Task`.
  Data is forwarded element by element. **Batching is to be implemented
  explicitly**. For example `Postgrex.stream/3` sends data in chunks
  by default.

  ## Stream function

  `t:stream_fun/0` must be a function that accepts one or two arguments.

  - The first argument is either `nil` or the last value received from a
  stream before recovery.
  - The second argument is an arbitrary term passed from `t:wrapper_fun/0`

  The function should return a `Stream` (although, any `Enumerable` could work).

  ## Example

      iex> gen_stream_f = fn
      ...>   nil -> Stream.iterate(1, fn x when x < 2 -> x + 1 end)
      ...>     x -> Stream.iterate(x + 1, &(&1+1))
      ...> end
      iex> RecoverableStream.run(gen_stream_f)
      ...> |> Stream.take(4)
      ...> |> Enum.into([])
      [1, 2, 3, 4]

  ## Options

  - `:max_retries` - the total number of times
  error recovery is performed before an error is propagated (defaults to `1`).

    Retries counter is **not** reset upon a successful recovery!

  - `:timeout_fun` - function called with current retry attempt (number) and results in timeout taken
    before next retry attempt is carried out (defaults to `nil`, i.e., no timeout)

  - `:wrapper_fun` -- is a function that wraps a stream reducer running
     inside a `Task` (defaults to `fun f -> f.(%{}) end`).

     Useful when the `t:stream_fun/0` must be run within a certain
     context. E.g. `Postgrex.stream/3` only works inside
     `Postgrex.transaction/3`.

  - `:task_supervisor` - either pid or a name of `Task.Supervisor`
     to supervise a stream-reducer `Task`.
     (defaults to `RecoverableStream.TaskPool`)

     See `RecoverableStream.TasksPool.child_spec/1` for details.


     See [Readme](./readme.html#a-practical-example)
     for a more elaborate example.
  """
  def run(stream_fun, options \\ [])
      when is_function(stream_fun, 1) or is_function(stream_fun, 2) do
    opts = NimbleOptions.validate!(options, options_schema())

    context = %Context{
      supervisor: opts[:task_supervisor],
      max_retries: opts[:max_retries],
      stream_fun: stream_fun,
      wrapper_fun: opts[:wrapper_fun],
      timeout_fun: opts[:timeout_fun]
    }

    # TODO: reimplement as proper Enumerable?
    Stream.resource(
      fn -> start_fun(context) end,
      &next_fun/1,
      &after_fun/1
    )
  end

  ## Stream.resource functions
  defp start_fun(%Context{stream_fun: stream_fun} = ctx) do
    owner = self()
    reply_ref = make_ref()

    task =
      Task.Supervisor.async_nolink(ctx.supervisor, fn ->
        ctx.wrapper_fun.(fn stream_arg ->
          :erlang.fun_info(stream_fun)[:arity]
          |> case do
            1 -> stream_fun.(ctx.last_value)
            2 -> stream_fun.(ctx.last_value, stream_arg)
          end
          |> stream_reducer(owner, reply_ref)
        end)
      end)

    %Context{ctx | task: task, reply_ref: reply_ref}
  end

  defp next_fun(ctx) do
    %{
      task: %Task{ref: tref, pid: tpid},
      reply_ref: rref,
      attempt: attempt,
      max_retries: max_retries
    } = ctx

    send(tpid, {:ready, rref})

    receive do
      {^tref, {:done, ^rref}} ->
        Process.demonitor(tref, [:flush])
        {:halt, ctx}

      # TODO add an optional retries reset
      {:data, ^rref, el} ->
        {[el], %{ctx | last_value: el}}

      {:DOWN, ^tref, _, _, :normal} ->
        {:halt, ctx}

      {:DOWN, ^tref, _, _, reason} when attempt > max_retries ->
        exit({reason, {__MODULE__, :next_fun, ctx}})

      {:DOWN, ^tref, _, _, _reason} ->
        apply_timeout(ctx)
        {[], start_fun(%Context{ctx | attempt: attempt + 1})}
    end
  end

  defp after_fun(%{task: %Task{ref: tref, pid: tpid}, reply_ref: rref} = ctx) do
    send(tpid, {:done, rref})

    receive do
      {:DOWN, ^tref, _, _, :normal} ->
        :ok

      {:DOWN, ^tref, _, _, reason} ->
        exit({reason, {__MODULE__, :after_fun, ctx}})
    after
      100 ->
        Process.demonitor(tref, [:flush])
        Task.Supervisor.terminate_child(TasksPool, tpid)
    end
  end

  defp apply_timeout(%Context{timeout_fun: nil}), do: :ok

  defp apply_timeout(%Context{timeout_fun: fun, attempt: attempt})
       when is_function(fun, 1) do
    case fun.(attempt) do
      non_pos when is_integer(non_pos) and non_pos <= 0 ->
        :ok

      i when is_integer(i) and i > 0 ->
        :timer.sleep(i)

      other ->
        raise ArgumentError, message: "invalid retry timeout value: #{inspect(other)}"
    end
  end

  defp stream_reducer(stream, owner, reply_ref) do
    mon_ref = Process.monitor(owner)

    # TODO consider adding a timeout for sending elements
    Enum.each(stream, fn el ->
      send_data(owner, mon_ref, reply_ref, el)
    end)

    {:done, reply_ref}
  end

  defp send_data(pid, mon_ref, reply_ref, data) do
    receive do
      {:done, ^reply_ref} ->
        exit(:normal)

      {:ready, ^reply_ref} ->
        send(pid, {:data, reply_ref, data})

      {:DOWN, ^mon_ref, _, ^pid, reason} ->
        exit(reason)
    end
  end
end
