defmodule Elasticsearch.DataCase do
  @moduledoc false

  # This module defines the setup for tests requiring
  # access to the application's data layer.
  #
  # You may define functions here to be used as helpers in
  # your tests.
  #
  # Finally, if the test case interacts with the database,
  # it cannot be async. For this reason, every test runs
  # inside a transaction which is reset at the beginning
  # of the test unless the test case is marked as async.

  use ExUnit.CaseTemplate
  import Ecto.Query

  alias Elasticsearch.Test.Cluster, as: TestCluster

  require Logger

  using do
    quote do
      alias Elasticsearch.Test.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Elasticsearch.DataCase
    end
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Elasticsearch.Test.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Elasticsearch.Test.Repo, {:shared, self()})
    end

    Logger.configure(level: :warning)

    on_exit(fn ->
      clean_index(TestCluster, "posts")
    end)

    :ok
  end

  defp clean_index(cluster, index) do
    _ = Elasticsearch.delete!(cluster, "/#{index}*")

    case Task.yield(Task.async(fn -> await_delete(cluster, index) end), 1500) do
      {:ok, _} ->
        :ok

      {:exit, msg} ->
        Logger.error("Failed to delete index #{index} with error #{msg}")

      nil ->
        Logger.error("Failed to delete index: #{index}")
    end

    :ok
  end

  defp await_delete(cluster, index) do
    case Elasticsearch.get(cluster, "/#{index}/_stats") do
      {:ok, _} ->
        :timer.sleep(10)
        await_delete(cluster, index)

      {:error, %Elasticsearch.Exception{status: 404}} ->
        :ok

      {:error, reason} ->
        raise(reason)
    end
  end

  def populate_posts_table(quantity \\ 10_000) do
    posts =
      [%{title: "Example Post", author: "John Smith"}]
      |> Stream.cycle()
      |> Enum.take(quantity)

    Elasticsearch.Test.Repo.insert_all("posts", posts)
  end

  def random_post_id do
    case Elasticsearch.Test.Repo.one(
           from(
             p in Post,
             order_by: fragment("RANDOM()"),
             limit: 1
           )
         ) do
      nil -> nil
      post -> post.id
    end
  end

  def populate_comments_table(quantity \\ 10) do
    comments =
      0..quantity
      |> Enum.map(fn _ ->
        %{
          body: "Example Comment",
          author: "Jane Doe",
          post_id: random_post_id()
        }
      end)

    Elasticsearch.Test.Repo.insert_all("comments", comments)
  end
end
