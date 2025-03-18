defmodule Elasticsearch.Namespace do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def set_pid_namespace(pid, namespace) when is_pid(pid) and is_binary(namespace) do
    GenServer.call(__MODULE__, {:set, pid, namespace})
  end

  def get_pid_namespace(pid) when is_pid(pid) do
    GenServer.call(__MODULE__, {:get, pid})
  end

  def clear_pid_namespace(pid) when is_pid(pid) do
    GenServer.call(__MODULE__, {:clear, pid})
  end


  @impl true
  def handle_call({:set, pid, namespace}, _from, state) do
    Process.monitor(pid)
    {:reply, :ok, Map.put(state, pid, namespace)}
  end

  def handle_call({:clear, pid}, _from, state) do
    {:reply, :ok, Map.delete(state, pid)}
  end

  def handle_call({:get, pid}, _from, state) do
    {:reply, Map.get(state, pid), state}
  end

  @impl true
  # Clear the namespace for the process when the process dies,
  # so we don't keep filling up the state.
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, Map.delete(state, pid)}
  end

  defp flatten_namespace([]), do: nil
  defp flatten_namespace(namespace), do: Enum.join(namespace, "-")
end
