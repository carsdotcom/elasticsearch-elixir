defmodule Elasticsearch.Cluster.ConfigTest do
  use ExUnit.Case

  alias Elasticsearch.Cluster.Config

  describe ".build/2" do
    setup do
      on_exit(fn ->
        Application.delete_env(:otp_app, Test.Module)
      end)
    end

    test "handles nil as first argument" do
      assert %{key: "value"} = Config.build(nil, %{key: "value"})
    end

    test "set a default Finch supervisor name" do
      config = Config.build(:otp_app, Test.Module, %{key: "value"})
      assert Keyword.get(config.http_supervisor_options, :name) == Test.Module.FinchSupervisor
    end

    test "persists all http adapter config" do
      config =
        Config.build(:otp_app, Test.Module, %{
          http_supervisor_options: [
            name: My.Test.SupervisorFinch,
            pools: %{
              "http://localhost:1234/path/gets/ignored?true" => [
                size: 99,
                protocol: :http2,
                count: 3
              ],
              :default => [size: 300]
            }
          ]
        })

      assert Keyword.get(config.http_supervisor_options, :name) == My.Test.SupervisorFinch

      assert config.http_supervisor_options
             |> Keyword.get(:pools)
             |> Map.get("http://localhost:1234/path/gets/ignored?true") ==
               [size: 99, protocol: :http2, count: 3]
    end

    test "http_supervisor_options are merged function config overrides app env config" do
      Application.put_env(:otp_app, Test.Module, %{
        http_supervisor_options: [
          name: This.ConfigName.Loses,
          pools: %{
            "http://localhost:1234/path/gets/ignored?true" => [
              size: 999,
              protocol: :http1,
              count: 333
            ],
            :default => [size: 300]
          }
        ]
      })

      config =
        Config.build(:otp_app, Test.Module, %{
          http_supervisor_options: [
            name: My.WinnerName.SupervisorFinch
          ]
        })

      assert Keyword.get(config.http_supervisor_options, :name) == My.WinnerName.SupervisorFinch

      assert config.http_supervisor_options
             |> Keyword.get(:pools)
             |> Map.get("http://localhost:1234/path/gets/ignored?true") ==
               [size: 999, protocol: :http1, count: 333]
    end

    test "when app env has values and config has http_supervisor_options without a name" do
      Application.put_env(:otp_app, Test.Module, %{
        http_supervisor_options: [
          pools: %{
            "http://localhost:1234/path/gets/ignored?true" => [
              size: 999,
              protocol: :http1,
              count: 333
            ],
            :default => [size: 300]
          }
        ]
      })

      config = Config.build(:otp_app, Test.Module, [])

      assert Keyword.get(config.http_supervisor_options, :name) == Test.Module.FinchSupervisor

      assert config.http_supervisor_options
             |> Keyword.get(:pools)
             |> Map.get("http://localhost:1234/path/gets/ignored?true") ==
               [size: 999, protocol: :http1, count: 333]
    end

    test "adds the default name if omitted" do
      config =
        Config.build(:otp_app, Test.Module, %{
          http_supervisor_options: [
            pools: %{
              :default => [size: 300]
            }
          ]
        })

      assert Keyword.get(config.http_supervisor_options, :pools) == %{:default => [size: 300]}
      assert Keyword.get(config.http_supervisor_options, :name) == Test.Module.FinchSupervisor
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
    end
  end
end
