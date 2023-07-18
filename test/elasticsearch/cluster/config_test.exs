defmodule Elasticsearch.Cluster.ConfigTest do
  use ExUnit.Case

  alias Elasticsearch.Cluster.Config

  describe ".build/2" do
    test "handles nil as first argument" do
      assert %{key: "value"} = Config.build(nil, %{key: "value"})
    end
  end

  describe "validate/1" do
    test "when valid" do
      assert {:ok, _} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch
               })

      assert {:ok, _} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 password: "123456",
                 username: ":bob"
               })
    end

    test "when valid with indexes" do
      assert {:ok, _} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: 5000,
                     bulk_wait_interval: 0,
                     settings: "test/support/settings/posts.json",
                     sources: [Post],
                     store: Elasticsearch.Test.Store
                   }
                 }
               })
    end

    test "when invalid with indexes" do
      assert {
               :error,
               %{
                 settings: [{"must be valid", [validation: :by]}],
                 sources: [
                   {"must be present", [validation: :presence]},
                   {"must be valid", [validation: :by]}
                 ],
                 store: [
                   {"must be present", [validation: :presence]},
                   {"must be valid", [validation: :by]}
                 ]
               }
             } =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: -1,
                     bulk_wait_interval: 0
                   }
                 }
               })

      assert {
               :error,
               %{
                 settings: [{"must be valid", [validation: :by]}],
                 sources: [
                   {"must be present", [validation: :presence]},
                   {"must be valid", [validation: :by]}
                 ],
                 store: [
                   {"must be present", [validation: :presence]},
                   {"must be valid", [validation: :by]}
                 ]
               }
             } =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: 1,
                     bulk_wait_interval: 0
                   }
                 }
               })

      assert {:error,
              %{
                settings: [{"must be valid", [validation: :by]}],
                store: [
                  {"must be present", [validation: :presence]},
                  {"must be valid", [validation: :by]}
                ]
              }} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: 1,
                     bulk_wait_interval: 0,
                     sources: [Post]
                   }
                 }
               })

      assert {:error,
              %{
                store: [
                  {"must be present", [validation: :presence]},
                  {"must be valid", [validation: :by]}
                ]
              }} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: 1,
                     bulk_wait_interval: 0,
                     sources: [Post],
                     settings: %{}
                   }
                 }
               })

      assert {:error, %{store: [{"must be valid", [validation: :by]}]}} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 indexes: %{
                   posts: %{
                     bulk_page_size: 1,
                     bulk_wait_interval: 0,
                     sources: [Post],
                     settings: %{},
                     store: :SomeInvalidModule
                   }
                 }
               })
    end

    test "when invalid" do
      assert {:error,
              %{
                api: [
                  {"must be present", [validation: :presence]},
                  {"must be valid", [validation: :by]}
                ]
              }} = Config.validate(%{url: "http://localhost:9200"})

      assert {:error, %{username: [{"must be present", [validation: :presence]}]}} =
               Config.validate(%{
                 url: "http://localhost:9200",
                 api: Elasticsearch,
                 password: 123_456
               })

      assert {:error, %{url: [{"must be valid", [validation: :by]}]}} =
               Config.validate(%{url: "NOTtp://localhost:9200", api: Elasticsearch})

      # assert message == "invalid value for :url option: invalid URL format"
    end
  end
end
