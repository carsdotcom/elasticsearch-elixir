defmodule ElasticsearchTest do
  alias Elasticsearch.Namespace

  def new_process_namespace(pid) when is_pid(pid) do
    pid
    |> :erlang.phash2()
    |> to_string()
  end
end
