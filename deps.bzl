load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_dependencies():
    go_repository(
        name = "co_honnef_go_tools",
        importpath = "honnef.co/go/tools",
        sum = "h1:/hemPrYIhOhy8zYrNj+069zDB68us2sMGsfkFJO0iZs=",
        version = "v0.0.0-20190523083050-ea95bdfd59fc",
    )
    go_repository(
        name = "com_github_antihax_optional",
        importpath = "github.com/antihax/optional",
        sum = "h1:xK2lYat7ZLaVVcIuj82J8kIro4V6kDe0AUDFboUCwcg=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_armon_consul_api",
        importpath = "github.com/armon/consul-api",
        sum = "h1:G1bPvciwNyF7IUmKXNt9Ak3m6u9DE1rF+RmtIkBpVdA=",
        version = "v0.0.0-20180202201655-eb2c6b5be1b6",
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        importpath = "github.com/aws/aws-sdk-go",
        sum = "h1:ZPB9QhwxmMIEC8ja0DdFowOl5fODWaZ6s2cZ40fx6r8=",
        version = "v1.44.282",
    )
    go_repository(
        name = "com_github_azure_azure_pipeline_go",
        importpath = "github.com/Azure/azure-pipeline-go",
        sum = "h1:7U9HBg1JFK3jHl5qmo4CTZKFTVgMwdFHMVtCdfBE21U=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_github_azure_azure_storage_blob_go",
        importpath = "github.com/Azure/azure-storage-blob-go",
        sum = "h1:rXtgp8tN1p29GvpGgfJetavIG0V7OgcSXPpwp3tx6qk=",
        version = "v0.15.0",
    )
    go_repository(
        name = "com_github_azure_go_autorest",
        importpath = "github.com/Azure/go-autorest",
        sum = "h1:V5VMDjClD3GiElqLWO7mz2MxNAK/vTfRHdAubSIPRgs=",
        version = "v14.2.0+incompatible",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_adal",
        importpath = "github.com/Azure/go-autorest/autorest/adal",
        sum = "h1:Mp5hbtOePIzM8pJVRa3YLrWWmZtoxRXqUEzCfJt3+/Q=",
        version = "v0.9.13",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_date",
        importpath = "github.com/Azure/go-autorest/autorest/date",
        sum = "h1:7gUk1U5M/CQbp9WoqinNzJar+8KY+LPI6wiWrP/myHw=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_mocks",
        importpath = "github.com/Azure/go-autorest/autorest/mocks",
        sum = "h1:K0laFcLE6VLTOwNgSxaGbUcLPuGXlNkbVvq4cW4nIHk=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_azure_go_autorest_logger",
        importpath = "github.com/Azure/go-autorest/logger",
        sum = "h1:IG7i4p/mDa2Ce4TRyAO8IHnVhAVF3RFU+ZtXWSmf4Tg=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_azure_go_autorest_tracing",
        importpath = "github.com/Azure/go-autorest/tracing",
        sum = "h1:TYi4+3m5t6K48TGI9AUdb+IzbnSxvnvUMfuitfgcfuo=",
        version = "v0.6.0",
    )

    go_repository(
        name = "com_github_benbjohnson_clock",
        importpath = "github.com/benbjohnson/clock",
        sum = "h1:Q92kusRqC1XV2MjkWETPvjJVqKetz1OzxZB7mHJLju8=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        importpath = "github.com/BurntSushi/toml",
        sum = "h1:WXkYYl6Yr3qBf1K79EBnL4mak0OimBfB0XUf9Vl28OQ=",
        version = "v0.3.1",
    )
    go_repository(
        name = "com_github_bwmarrin_snowflake",
        importpath = "github.com/bwmarrin/snowflake",
        sum = "h1:xm67bEhkKh6ij1790JB83OujPR5CzNe8QuQqAgISZN0=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sum = "h1:iKLQ0xPNFxR/2hzXZMrBo8f1j86j5WHzznCCQxV/b8g=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        importpath = "github.com/cespare/xxhash",
        sum = "h1:a6HrQnmkObjyL+Gs60czilIUGqrzKutQD6XZog3p+ko=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        importpath = "github.com/cespare/xxhash/v2",
        sum = "h1:DC2CZ1Ep5Y4k3ZQ899DldepgrayRUGE6BBZ/cd9Cj44=",
        version = "v2.2.0",
    )
    go_repository(
        name = "com_github_client9_misspell",
        importpath = "github.com/client9/misspell",
        sum = "h1:ta993UF76GwbvJcIo3Y68y/M3WxlpEHPWIGDkJYwzJI=",
        version = "v0.3.4",
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        importpath = "github.com/cncf/udpa/go",
        sum = "h1:QQ3GSy+MqSHxm/d8nCtnAiZdYFd45cYZPs8vOOIYKfk=",
        version = "v0.0.0-20220112060539-c52dc94e7fbe",
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        importpath = "github.com/cncf/xds/go",
        sum = "h1:58f1tJ1ra+zFINPlwLWvQsR9CzAKt2e+EWV2yX9oXQ4=",
        version = "v0.0.0-20230310173818-32f1caf87195",
    )
    go_repository(
        name = "com_github_coreos_etcd",
        importpath = "github.com/coreos/etcd",
        sum = "h1:jFneRYjIvLMLhDLCzuTuU4rSJUjRplcJQ7pD7MnhC04=",
        version = "v3.3.10+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_etcd",
        importpath = "github.com/coreos/go-etcd",
        sum = "h1:bXhRBIXoTm9BYHS3gE0TtQuyNZyeEMux2sDi4oo5YOo=",
        version = "v2.0.0+incompatible",
    )
    go_repository(
        name = "com_github_coreos_go_semver",
        importpath = "github.com/coreos/go-semver",
        sum = "h1:3Jm3tLmsgAYcjC+4Up7hJrFBPr+n7rAqYeSw/SZazuY=",
        version = "v0.2.0",
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man",
        importpath = "github.com/cpuguy83/go-md2man",
        sum = "h1:BSKMNlYxDvnunlTymqtgONjNnaRV1sTpcovwwjF22jk=",
        version = "v1.0.10",
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        importpath = "github.com/davecgh/go-spew",
        sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_detailyang_go_fallocate",
        importpath = "github.com/detailyang/go-fallocate",
        sum = "h1:lj77EKYUpYXTd8CD/+QMIf8b6OIOTsfEBSXiAzuEHTU=",
        version = "v0.0.0-20180908115635-432fa640bd2e",
    )
    go_repository(
        name = "com_github_dgraph_io_badger_v3",
        importpath = "github.com/dgraph-io/badger/v3",
        sum = "h1:ylPa6qzbjYRQMU6jokoj4wzcaweHylt//CH0AKt0akg=",
        version = "v3.2103.5",
    )
    go_repository(
        name = "com_github_dgraph_io_ristretto",
        importpath = "github.com/dgraph-io/ristretto",
        sum = "h1:6CWw5tJNgpegArSHpNHJKldNeq03FQCwYvfMVWajOK8=",
        version = "v0.1.1",
    )
    go_repository(
        name = "com_github_dgryski_go_farm",
        importpath = "github.com/dgryski/go-farm",
        sum = "h1:tdlZCpZ/P9DhczCTSixgIKmwPv6+wP5DGjqLYw5SUiA=",
        version = "v0.0.0-20190423205320-6a90982ecee2",
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        importpath = "github.com/dustin/go-humanize",
        sum = "h1:VSnTsYCnlFHaM2/igO1h6X3HA71jcobQuxemgkq4zYo=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        importpath = "github.com/envoyproxy/go-control-plane",
        sum = "h1:jtLewhRR2vMRNnq2ZZUoCjUlgut+Y0+sDDWPOfwOi1o=",
        version = "v0.11.0",
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sum = "h1:oIfnZFdC0YhpNNEX+SuIqko4cqqVZeN9IGTrhZje83Y=",
        version = "v0.10.0",
    )
    go_repository(
        name = "com_github_form3tech_oss_jwt_go",
        importpath = "github.com/form3tech-oss/jwt-go",
        sum = "h1:TcekIExNqud5crz4xD2pavyTgWiPvpYe4Xau31I0PRk=",
        version = "v3.2.2+incompatible",
    )

    go_repository(
        name = "com_github_fsnotify_fsnotify",
        importpath = "github.com/fsnotify/fsnotify",
        sum = "h1:IXs+QLmnXW2CcXuY+8Mzv/fWEsPGWxqefPtCP5CnV9I=",
        version = "v1.4.7",
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        importpath = "github.com/ghodss/yaml",
        sum = "h1:wQHKEahhL6wmXdzwWG11gIVCkOv05bNOh+Rxn0yngAk=",
        version = "v1.0.0",
    )

    go_repository(
        name = "com_github_go_ole_go_ole",
        importpath = "github.com/go-ole/go-ole",
        sum = "h1:/Fpf6oFPoeFik9ty7siob0G6Ke8QvQEuVcuChpwXzpY=",
        version = "v1.2.6",
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        importpath = "github.com/gogo/protobuf",
        sum = "h1:Ov1cvc58UF3b5XjBnZv7+opcTcQFZebYjWzi34vdm4Q=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_golang_glog",
        importpath = "github.com/golang/glog",
        sum = "h1:/d3pCKDPWNnvIWe0vVUpNP32qc8U3PDVxySP/y360qE=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_golang_groupcache",
        importpath = "github.com/golang/groupcache",
        sum = "h1:oI5xCqsCo564l8iNU+DwB5epxmsaqB+rhGL0m5jtYqE=",
        version = "v0.0.0-20210331224755-41bb18bfe9da",
    )
    go_repository(
        name = "com_github_golang_mock",
        importpath = "github.com/golang/mock",
        sum = "h1:G5FRp8JnTd7RQH5kemVNlMeyXQAztQ3mOWV95KxsXH8=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        sum = "h1:KhyjKVUg7Usr/dYsdSqoFveMYd5ko72D+zANwlG1mmg=",
        version = "v1.5.3",
    )
    go_repository(
        name = "com_github_golang_snappy",
        importpath = "github.com/golang/snappy",
        sum = "h1:fHPg5GQYlCeLIPB9BZqMVR5nR9A+IM5zcgeTdjMYmLA=",
        version = "v0.0.3",
    )
    go_repository(
        name = "com_github_google_flatbuffers",
        importpath = "github.com/google/flatbuffers",
        sum = "h1:MVlul7pQNoDzWRLTw5imwYsl+usrS1TXG2H4jg6ImGw=",
        version = "v1.12.1",
    )
    go_repository(
        name = "com_github_google_go_cmp",
        importpath = "github.com/google/go-cmp",
        sum = "h1:O2Tfq5qg4qc4AmwVlvv0oLiVAGB7enBSJ2x2DqQFi38=",
        version = "v0.5.9",
    )
    go_repository(
        name = "com_github_google_martian_v3",
        importpath = "github.com/google/martian/v3",
        sum = "h1:IqNFLAmvJOgVlpdEBiQbDc2EwKW77amAycfTuWKdfvw=",
        version = "v3.3.2",
    )
    go_repository(
        name = "com_github_google_s2a_go",
        importpath = "github.com/google/s2a-go",
        sum = "h1:1kZ/sQM3srePvKs3tXAvQzo66XfcReoqFpIpIccE7Oc=",
        version = "v0.1.4",
    )

    go_repository(
        name = "com_github_google_uuid",
        importpath = "github.com/google/uuid",
        sum = "h1:t6JiXgmwXMjEs8VusXIJk2BXHsn+wx8BZdTaoZ5fu7I=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_googleapis_enterprise_certificate_proxy",
        importpath = "github.com/googleapis/enterprise-certificate-proxy",
        sum = "h1:uGy6JWR/uMIILU8wbf+OkstIrNiMjGpEIyhx8f6W7s4=",
        version = "v0.2.4",
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        importpath = "github.com/googleapis/gax-go/v2",
        sum = "h1:ebSgKfMxynOdxw8QQuFOKMgomqeLGPqNLQox2bo42zg=",
        version = "v2.10.0",
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        importpath = "github.com/grpc-ecosystem/grpc-gateway",
        sum = "h1:gmcG1KaJ57LophUzW0Hy8NmPhnMZb4M0+kPpLofRdBo=",
        version = "v1.16.0",
    )

    go_repository(
        name = "com_github_hashicorp_hcl",
        importpath = "github.com/hashicorp/hcl",
        sum = "h1:0Anlzjpi4vEasTeNFn2mLJgTSwt0+6sfsiTG8qcWGx4=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_hpcloud_tail",
        importpath = "github.com/hpcloud/tail",
        sum = "h1:nfCOvKYfkgYP8hkirhJocXT2+zOD8yUNjXaWfTlyFKI=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        importpath = "github.com/inconshreveable/mousetrap",
        sum = "h1:Z8tu5sraLXCXIcARxBp/8cbvlwVa7Z1NHg9XEKhtSvM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_jacobsa_fuse",
        importpath = "github.com/jacobsa/fuse",
        sum = "h1:TqeYJCx5m/BvJQvoy4gQt/RdjG4p+C/qI0RVhhUZC0Q=",
        version = "v0.0.0-20230425120156-b7182e0d0b51",
    )
    go_repository(
        name = "com_github_jacobsa_oglematchers",
        importpath = "github.com/jacobsa/oglematchers",
        sum = "h1:9GCSedGjMcLZCrusBZuo4tyKLpKUPenUUqi34AkuFmA=",
        version = "v0.0.0-20150720000706-141901ea67cd",
    )
    go_repository(
        name = "com_github_jacobsa_oglemock",
        importpath = "github.com/jacobsa/oglemock",
        sum = "h1:2xRHTvkpJ5zJmglXLRqHiZQNjUoOkhUyhTAhEQvPAWw=",
        version = "v0.0.0-20150831005832-e94d794d06ff",
    )
    go_repository(
        name = "com_github_jacobsa_ogletest",
        importpath = "github.com/jacobsa/ogletest",
        sum = "h1:BMb8s3ENQLt5ulwVIHVDWFHp8eIXmbfSExkvdn9qMXI=",
        version = "v0.0.0-20170503003838-80d50a735a11",
    )
    go_repository(
        name = "com_github_jacobsa_reqtrace",
        importpath = "github.com/jacobsa/reqtrace",
        sum = "h1:uSWBjJdMf47kQlXMwWEfmc864bA1wAC+Kl3ApryuG9Y=",
        version = "v0.0.0-20150505043853-245c9e0234cb",
    )
    go_repository(
        name = "com_github_jacobsa_syncutil",
        importpath = "github.com/jacobsa/syncutil",
        sum = "h1:+gHfvQxomE6fI4zg7QYyaGDCnuw2wylD4i6yzrQvAmY=",
        version = "v0.0.0-20180201203307-228ac8e5a6c3",
    )
    go_repository(
        name = "com_github_jacobsa_timeutil",
        importpath = "github.com/jacobsa/timeutil",
        sum = "h1:XKHJmHcgU9glxk3eLPiRZT5VFSHJitVTnMj/EgIoXC4=",
        version = "v0.0.0-20170205232429-577e5acbbcf6",
    )
    go_repository(
        name = "com_github_jinzhu_copier",
        importpath = "github.com/jinzhu/copier",
        sum = "h1:GlvfUwHk62RokgqVNvYsku0TATCF7bAHVwEXoBh3iJg=",
        version = "v0.3.5",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        importpath = "github.com/jmespath/go-jmespath",
        sum = "h1:BEgLn5cpjn8UN1mAw4NjwDrS35OdebyEtFe+9YPoQUg=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sum = "h1:shLQSRRSCCPj3f2gpwzGwWFoC7ycTf1rcQZHOlsJ6N8=",
        version = "v1.5.1",
    )

    go_repository(
        name = "com_github_kisielk_errcheck",
        importpath = "github.com/kisielk/errcheck",
        sum = "h1:e8esj/e4R+SAOwFwN+n3zr0nYeCyeweozKfO23MvHzY=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        importpath = "github.com/kisielk/gotool",
        sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        sum = "h1:G5AfA94pHPysR56qqrkO2pxEexdDzrpFJ6yt/VqWxVU=",
        version = "v1.12.3",
    )
    go_repository(
        name = "com_github_kr_pretty",
        importpath = "github.com/kr/pretty",
        sum = "h1:Fmg33tUaq4/8ym9TJN1x7sLJnHVwhP33CNkpYV/7rwI=",
        version = "v0.2.1",
    )
    go_repository(
        name = "com_github_kr_pty",
        importpath = "github.com/kr/pty",
        sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
        version = "v1.1.1",
    )
    go_repository(
        name = "com_github_kr_text",
        importpath = "github.com/kr/text",
        sum = "h1:45sCR5RtlFHMR4UwH9sdQ5TC8v0qDQCHnXt+kaKSTVE=",
        version = "v0.1.0",
    )
    go_repository(
        name = "com_github_kylelemons_godebug",
        importpath = "github.com/kylelemons/godebug",
        sum = "h1:RPNrshWIDI6G2gRW9EHilWtl7Z6Sb1BR0xunSBf0SNc=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_logrusorgru_aurora",
        importpath = "github.com/logrusorgru/aurora",
        sum = "h1:9MlwzLdW7QSDrhDjFlsEYmxpFyIoXmYRon3dt0io31k=",
        version = "v0.0.0-20181002194514-a7b3b318ed4e",
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        importpath = "github.com/lufia/plan9stats",
        sum = "h1:6E+4a0GO5zZEnZ81pIr0yLvtUWk2if982qA3F3QD6H4=",
        version = "v0.0.0-20211012122336-39d0f177ccd0",
    )
    go_repository(
        name = "com_github_magiconair_properties",
        importpath = "github.com/magiconair/properties",
        sum = "h1:LLgXmsheXeRoUOBOjtwPQCWIYqM/LU1ayDtDePerRcY=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_github_mattn_go_ieproxy",
        importpath = "github.com/mattn/go-ieproxy",
        sum = "h1:qiyop7gCflfhwCzGyeT0gro3sF9AIg9HU98JORTkqfI=",
        version = "v0.0.1",
    )

    go_repository(
        name = "com_github_mitchellh_go_homedir",
        importpath = "github.com/mitchellh/go-homedir",
        sum = "h1:lukF9ziXFxDFPkA1vsr5zpc1XuPDn/wFntq5mG+4E0Y=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_mitchellh_mapstructure",
        importpath = "github.com/mitchellh/mapstructure",
        sum = "h1:fmNYVwqnSfB9mZU6OS2O6GsXM+wcskZDuKQzvN1EDeE=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_nutsdb_nutsdb",
        importpath = "github.com/nutsdb/nutsdb",
        sum = "h1:6P7EJat2PyVhRu51KMmFu5N851UupfpPBHAqctRm3/4=",
        version = "v0.12.0",
    )
    go_repository(
        name = "com_github_oneofone_xxhash",
        importpath = "github.com/OneOfOne/xxhash",
        sum = "h1:KMrpdQIwFcEqXDklaen+P1axHaj9BSKzvpUUfnHldSE=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_onsi_ginkgo",
        importpath = "github.com/onsi/ginkgo",
        sum = "h1:WSHQ+IS43OoUrWtD1/bbclrwK8TTH5hzp+umCiuxHgs=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_github_onsi_gomega",
        importpath = "github.com/onsi/gomega",
        sum = "h1:RE1xgDvH7imwFD45h+u2SgIfERHlS2yNG4DObb5BSKU=",
        version = "v1.4.3",
    )
    go_repository(
        name = "com_github_pelletier_go_toml",
        importpath = "github.com/pelletier/go-toml",
        sum = "h1:T5zMGML61Wp+FlcbWjRDT7yAxhJNAiPPLOFECq181zc=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_pkg_errors",
        importpath = "github.com/pkg/errors",
        sum = "h1:FEBLx1zS214owpjy7qsBeixbURkuhQAwrK5UwLGTwt4=",
        version = "v0.9.1",
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath = "github.com/pmezard/go-difflib",
        sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_power_devops_perfstat",
        importpath = "github.com/power-devops/perfstat",
        sum = "h1:ncq/mPwQF4JjgDlrVEn3C11VoGHZN7m8qihwgMEtzYw=",
        version = "v0.0.0-20210106213030-5aafc221ea8c",
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        importpath = "github.com/prometheus/client_model",
        sum = "h1:gQz4mCbXsO+nc9n1hCxHcGA3Zx3Eo+UHZoInFGUIXNM=",
        version = "v0.0.0-20190812154241-14fe0d1b01d4",
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        importpath = "github.com/rogpeppe/fastuuid",
        sum = "h1:Ppwyp6VYCF1nvBTXL3trRso7mXMlRrw9ooo375wvi2s=",
        version = "v1.2.0",
    )

    go_repository(
        name = "com_github_russross_blackfriday",
        importpath = "github.com/russross/blackfriday",
        sum = "h1:HyvC0ARfnZBqnXwABFeSZHpKvJHJJfPz81GNueLj0oo=",
        version = "v1.5.2",
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        importpath = "github.com/shirou/gopsutil/v3",
        sum = "h1:hZwmDxZs7Ewt75DV81r4pFMqbq+di2cbt9FsQBqLD2o=",
        version = "v3.23.4",
    )
    go_repository(
        name = "com_github_shoenig_go_m1cpu",
        importpath = "github.com/shoenig/go-m1cpu",
        sum = "h1:LF57Z/Fpb/WdGLjt2HZilNnmZOxg/q2bSKTQhgbrLrQ=",
        version = "v0.1.5",
    )
    go_repository(
        name = "com_github_shoenig_test",
        importpath = "github.com/shoenig/test",
        sum = "h1:GVXWJFk9PiOjN0KoJ7VrJGH6uLPnqxR7/fe3HUPfE0c=",
        version = "v0.6.3",
    )
    go_repository(
        name = "com_github_spaolacci_murmur3",
        importpath = "github.com/spaolacci/murmur3",
        sum = "h1:7c1g84S4BPRrfL5Xrdp6fOJ206sU9y293DDHaoy0bLI=",
        version = "v1.1.0",
    )
    go_repository(
        name = "com_github_spf13_afero",
        importpath = "github.com/spf13/afero",
        sum = "h1:m8/z1t7/fwjysjQRYbP0RD+bUIF/8tJwPdEZsI83ACI=",
        version = "v1.1.2",
    )
    go_repository(
        name = "com_github_spf13_cast",
        importpath = "github.com/spf13/cast",
        sum = "h1:oget//CVOEoFewqQxwr0Ej5yjygnqGkvggSE/gB35Q8=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_github_spf13_cobra",
        importpath = "github.com/spf13/cobra",
        sum = "h1:f0B+LkLX6DtmRH1isoNA9VTtNUK9K8xYd28JNNfOv/s=",
        version = "v0.0.5",
    )
    go_repository(
        name = "com_github_spf13_jwalterweatherman",
        importpath = "github.com/spf13/jwalterweatherman",
        sum = "h1:XHEdyB+EcvlqZamSM4ZOMGlc93t6AcsBEu9Gc1vn7yk=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_spf13_pflag",
        importpath = "github.com/spf13/pflag",
        sum = "h1:zPAT6CGy6wXeQ7NtTnaTerfKOsV6V6F8agHXFiazDkg=",
        version = "v1.0.3",
    )
    go_repository(
        name = "com_github_spf13_viper",
        importpath = "github.com/spf13/viper",
        sum = "h1:VUFqw5KcqRf7i70GOzW7N+Q7+gxVBkSSqiXB12+JQ4M=",
        version = "v1.3.2",
    )
    go_repository(
        name = "com_github_stretchr_objx",
        importpath = "github.com/stretchr/objx",
        sum = "h1:1zr/of2m5FGMsad5YfcqgdqdWrIhu+EBEJRhR1U7z/c=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_github_stretchr_testify",
        importpath = "github.com/stretchr/testify",
        sum = "h1:+h33VjcLVPDHtOdpUCuF+7gSuG3yGIftsP1YvFihtJ8=",
        version = "v1.8.2",
    )
    go_repository(
        name = "com_github_syndtr_goleveldb",
        importpath = "github.com/syndtr/goleveldb",
        sum = "h1:fBdIW9lB4Iz0n9khmH8w27SJ3QEJ7+IgjPEwGSZiFdE=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        importpath = "github.com/tklauser/go-sysconf",
        sum = "h1:89WgdJhk5SNwJfu+GKyYveZ4IaJ7xAkecBo+KdJV0CM=",
        version = "v0.3.11",
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        importpath = "github.com/tklauser/numcpus",
        sum = "h1:kebhY2Qt+3U6RNK7UqpYNA+tJ23IBEGKkB7JQBfDYms=",
        version = "v0.6.0",
    )
    go_repository(
        name = "com_github_ugorji_go_codec",
        importpath = "github.com/ugorji/go/codec",
        sum = "h1:3SVOIvH7Ae1KRYyQWRjXWJEA9sS/c/pjvH++55Gr648=",
        version = "v0.0.0-20181204163529-d75b2dcb6bc8",
    )
    go_repository(
        name = "com_github_xordataexchange_crypt",
        importpath = "github.com/xordataexchange/crypt",
        sum = "h1:ESFSdwYZvkeru3RtdrYueztKhOBCSAAzS4Gf+k0tEow=",
        version = "v0.0.3-0.20170626215501-b2862e3d0a77",
    )
    go_repository(
        name = "com_github_xujiajun_gorouter",
        importpath = "github.com/xujiajun/gorouter",
        sum = "h1:aPKfkzLHxPYRgr+irEE00SEOf78LHnxH/v4m8QiV51Y=",
        version = "v1.2.0",
    )
    go_repository(
        name = "com_github_xujiajun_mmap_go",
        importpath = "github.com/xujiajun/mmap-go",
        sum = "h1:7Se7ss1fLPPRW+ePgqGpCkfGIZzJV6JPq9Wq9iv/WHc=",
        version = "v1.0.1",
    )
    go_repository(
        name = "com_github_xujiajun_utils",
        importpath = "github.com/xujiajun/utils",
        sum = "h1:w0si+uee0iAaCJO9q86T6yrhdadgcsoNuh47LrUykzg=",
        version = "v0.0.0-20220904132955-5f7c5b914235",
    )
    go_repository(
        name = "com_github_yuin_goldmark",
        importpath = "github.com/yuin/goldmark",
        sum = "h1:fVcFKWvrslecOb/tg+Cc05dkeYx540o0FuFt3nUVDoE=",
        version = "v1.4.13",
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        importpath = "github.com/yusufpapurcu/wmi",
        sum = "h1:KBNDSne4vP5mbSWnJbO+51IMOXJB67QiYCSBrubbPRg=",
        version = "v1.2.2",
    )
    go_repository(
        name = "com_github_ztrue_tracerr",
        importpath = "github.com/ztrue/tracerr",
        sum = "h1:lDi6EgEYhPYPnKcjsYzmWw4EkFEoA/gfe+I9Y5f+h6Y=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go",
        importpath = "cloud.google.com/go",
        sum = "h1:Zc8gqp3+a9/Eyph2KDmcGaPtbKRIoqq4YTlL4NMD0Ys=",
        version = "v0.110.0",
    )
    go_repository(
        name = "com_google_cloud_go_accessapproval",
        importpath = "cloud.google.com/go/accessapproval",
        sum = "h1:x0cEHro/JFPd7eS4BlEWNTMecIj2HdXjOVB5BtvwER0=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_accesscontextmanager",
        importpath = "cloud.google.com/go/accesscontextmanager",
        sum = "h1:MG60JgnEoawHJrbWw0jGdv6HLNSf6gQvYRiXpuzqgEA=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_aiplatform",
        importpath = "cloud.google.com/go/aiplatform",
        sum = "h1:zTw+suCVchgZyO+k847wjzdVjWmrAuehxdvcZvJwfGg=",
        version = "v1.37.0",
    )
    go_repository(
        name = "com_google_cloud_go_analytics",
        importpath = "cloud.google.com/go/analytics",
        sum = "h1:LqAo3tAh2FU9+w/r7vc3hBjU23Kv7GhO/PDIW7kIYgM=",
        version = "v0.19.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigateway",
        importpath = "cloud.google.com/go/apigateway",
        sum = "h1:ZI9mVO7x3E9RK/BURm2p1aw9YTBSCQe3klmyP1WxWEg=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeconnect",
        importpath = "cloud.google.com/go/apigeeconnect",
        sum = "h1:sWOmgDyAsi1AZ48XRHcATC0tsi9SkPT7DA/+VCfkaeA=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_apigeeregistry",
        importpath = "cloud.google.com/go/apigeeregistry",
        sum = "h1:E43RdhhCxdlV+I161gUY2rI4eOaMzHTA5kNkvRsFXvc=",
        version = "v0.6.0",
    )

    go_repository(
        name = "com_google_cloud_go_appengine",
        importpath = "cloud.google.com/go/appengine",
        sum = "h1:aBGDKmRIaRRoWJ2tAoN0oVSHoWLhtO9aj/NvUyP4aYs=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_area120",
        importpath = "cloud.google.com/go/area120",
        sum = "h1:ugckkFh4XkHJMPhTIx0CyvdoBxmOpMe8rNs4Ok8GAag=",
        version = "v0.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_artifactregistry",
        importpath = "cloud.google.com/go/artifactregistry",
        sum = "h1:o1Q80vqEB6Qp8WLEH3b8FBLNUCrGQ4k5RFj0sn/sgO8=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_asset",
        importpath = "cloud.google.com/go/asset",
        sum = "h1:YAsssO08BqZ6mncbb6FPlj9h6ACS7bJQUOlzciSfbNk=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_assuredworkloads",
        importpath = "cloud.google.com/go/assuredworkloads",
        sum = "h1:VLGnVFta+N4WM+ASHbhc14ZOItOabDLH1MSoDv+Xuag=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_automl",
        importpath = "cloud.google.com/go/automl",
        sum = "h1:50VugllC+U4IGl3tDNcZaWvApHBTrn/TvyHDJ0wM+Uw=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_baremetalsolution",
        importpath = "cloud.google.com/go/baremetalsolution",
        sum = "h1:2AipdYXL0VxMboelTTw8c1UJ7gYu35LZYUbuRv9Q28s=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_batch",
        importpath = "cloud.google.com/go/batch",
        sum = "h1:YbMt0E6BtqeD5FvSv1d56jbVsWEzlGm55lYte+M6Mzs=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_beyondcorp",
        importpath = "cloud.google.com/go/beyondcorp",
        sum = "h1:UkY2BTZkEUAVrgqnSdOJ4p3y9ZRBPEe1LkjgC8Bj/Pc=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        importpath = "cloud.google.com/go/bigquery",
        sum = "h1:RscMV6LbnAmhAzD893Lv9nXXy2WCaJmbxYPWDLbGqNQ=",
        version = "v1.50.0",
    )
    go_repository(
        name = "com_google_cloud_go_billing",
        importpath = "cloud.google.com/go/billing",
        sum = "h1:JYj28UYF5w6VBAh0gQYlgHJ/OD1oA+JgW29YZQU+UHM=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_binaryauthorization",
        importpath = "cloud.google.com/go/binaryauthorization",
        sum = "h1:d3pMDBCCNivxt5a4eaV7FwL7cSH0H7RrEnFrTb1QKWs=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_certificatemanager",
        importpath = "cloud.google.com/go/certificatemanager",
        sum = "h1:5C5UWeSt8Jkgp7OWn2rCkLmYurar/vIWIoSQ2+LaTOc=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_channel",
        importpath = "cloud.google.com/go/channel",
        sum = "h1:GpcQY5UJKeOekYgsX3QXbzzAc/kRGtBq43fTmyKe6Uw=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_cloudbuild",
        importpath = "cloud.google.com/go/cloudbuild",
        sum = "h1:GHQCjV4WlPPVU/j3Rlpc8vNIDwThhd1U9qSY/NPZdko=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_clouddms",
        importpath = "cloud.google.com/go/clouddms",
        sum = "h1:E7v4TpDGUyEm1C/4KIrpVSOCTm0P6vWdHT0I4mostRA=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_cloudtasks",
        importpath = "cloud.google.com/go/cloudtasks",
        sum = "h1:uK5k6abf4yligFgYFnG0ni8msai/dSv6mDmiBulU0hU=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        importpath = "cloud.google.com/go/compute",
        sum = "h1:DcTwsFgGev/wV5+q8o2fzgcHOaac+DKGC91ZlvpsQds=",
        version = "v1.19.3",
    )
    go_repository(
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:mg4jlk7mCAj6xXp9UJ4fjI9VUI5rubuGBW5aJ7UnBMY=",
        version = "v0.2.3",
    )
    go_repository(
        name = "com_google_cloud_go_contactcenterinsights",
        importpath = "cloud.google.com/go/contactcenterinsights",
        sum = "h1:jXIpfcH/VYSE1SYcPzO0n1VVb+sAamiLOgCw45JbOQk=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_container",
        importpath = "cloud.google.com/go/container",
        sum = "h1:NKlY/wCDapfVZlbVVaeuu2UZZED5Dy1z4Zx1KhEzm8c=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_containeranalysis",
        importpath = "cloud.google.com/go/containeranalysis",
        sum = "h1:EQ4FFxNaEAg8PqQCO7bVQfWz9NVwZCUKaM1b3ycfx3U=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_datacatalog",
        importpath = "cloud.google.com/go/datacatalog",
        sum = "h1:4H5IJiyUE0X6ShQBqgFFZvGGcrwGVndTwUSLP4c52gw=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataflow",
        importpath = "cloud.google.com/go/dataflow",
        sum = "h1:eYyD9o/8Nm6EttsKZaEGD84xC17bNgSKCu0ZxwqUbpg=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataform",
        importpath = "cloud.google.com/go/dataform",
        sum = "h1:Dyk+fufup1FR6cbHjFpMuP4SfPiF3LI3JtoIIALoq48=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_datafusion",
        importpath = "cloud.google.com/go/datafusion",
        sum = "h1:sZjRnS3TWkGsu1LjYPFD/fHeMLZNXDK6PDHi2s2s/bk=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_datalabeling",
        importpath = "cloud.google.com/go/datalabeling",
        sum = "h1:ch4qA2yvddGRUrlfwrNJCr79qLqhS9QBwofPHfFlDIk=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataplex",
        importpath = "cloud.google.com/go/dataplex",
        sum = "h1:RvoZ5T7gySwm1CHzAw7yY1QwwqaGswunmqEssPxU/AM=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataproc",
        importpath = "cloud.google.com/go/dataproc",
        sum = "h1:W47qHL3W4BPkAIbk4SWmIERwsWBaNnWm0P2sdx3YgGU=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_dataqna",
        importpath = "cloud.google.com/go/dataqna",
        sum = "h1:yFzi/YU4YAdjyo7pXkBE2FeHbgz5OQQBVDdbErEHmVQ=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        importpath = "cloud.google.com/go/datastore",
        sum = "h1:iF6I/HaLs3Ado8uRKMvZRvF/ZLkWaWE9i8AiHzbC774=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_datastream",
        importpath = "cloud.google.com/go/datastream",
        sum = "h1:BBCBTnWMDwwEzQQmipUXxATa7Cm7CA/gKjKcR2w35T0=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_deploy",
        importpath = "cloud.google.com/go/deploy",
        sum = "h1:otshdKEbmsi1ELYeCKNYppwV0UH5xD05drSdBm7ouTk=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_dialogflow",
        importpath = "cloud.google.com/go/dialogflow",
        sum = "h1:uVlKKzp6G/VtSW0E7IH1Y5o0H48/UOCmqksG2riYCwQ=",
        version = "v1.32.0",
    )
    go_repository(
        name = "com_google_cloud_go_dlp",
        importpath = "cloud.google.com/go/dlp",
        sum = "h1:1JoJqezlgu6NWCroBxr4rOZnwNFILXr4cB9dMaSKO4A=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_documentai",
        importpath = "cloud.google.com/go/documentai",
        sum = "h1:KM3Xh0QQyyEdC8Gs2vhZfU+rt6OCPF0dwVwxKgLmWfI=",
        version = "v1.18.0",
    )
    go_repository(
        name = "com_google_cloud_go_domains",
        importpath = "cloud.google.com/go/domains",
        sum = "h1:2ti/o9tlWL4N+wIuWUNH+LbfgpwxPr8J1sv9RHA4bYQ=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_edgecontainer",
        importpath = "cloud.google.com/go/edgecontainer",
        sum = "h1:O0YVE5v+O0Q/ODXYsQHmHb+sYM8KNjGZw2pjX2Ws41c=",
        version = "v1.0.0",
    )
    go_repository(
        name = "com_google_cloud_go_errorreporting",
        importpath = "cloud.google.com/go/errorreporting",
        sum = "h1:kj1XEWMu8P0qlLhm3FwcaFsUvXChV/OraZwA70trRR0=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_essentialcontacts",
        importpath = "cloud.google.com/go/essentialcontacts",
        sum = "h1:gIzEhCoOT7bi+6QZqZIzX1Erj4SswMPIteNvYVlu+pM=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_eventarc",
        importpath = "cloud.google.com/go/eventarc",
        sum = "h1:fsJmNeqvqtk74FsaVDU6cH79lyZNCYP8Rrv7EhaB/PU=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_filestore",
        importpath = "cloud.google.com/go/filestore",
        sum = "h1:ckTEXN5towyTMu4q0uQ1Mde/JwTHur0gXs8oaIZnKfw=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        importpath = "cloud.google.com/go/firestore",
        sum = "h1:IBlRyxgGySXu5VuW0RgGFlTtLukSnNkpDiEOMkQkmpA=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_functions",
        importpath = "cloud.google.com/go/functions",
        sum = "h1:pPDqtsXG2g9HeOQLoquLbmvmb82Y4Ezdo1GXuotFoWg=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_gaming",
        importpath = "cloud.google.com/go/gaming",
        sum = "h1:7vEhFnZmd931Mo7sZ6pJy7uQPDxF7m7v8xtBheG08tc=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkebackup",
        importpath = "cloud.google.com/go/gkebackup",
        sum = "h1:za3QZvw6ujR0uyqkhomKKKNoXDyqYGPJies3voUK8DA=",
        version = "v0.4.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkeconnect",
        importpath = "cloud.google.com/go/gkeconnect",
        sum = "h1:gXYKciHS/Lgq0GJ5Kc9SzPA35NGc3yqu6SkjonpEr2Q=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkehub",
        importpath = "cloud.google.com/go/gkehub",
        sum = "h1:TqCSPsEBQ6oZSJgEYZ3XT8x2gUadbvfwI32YB0kuHCs=",
        version = "v0.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_gkemulticloud",
        importpath = "cloud.google.com/go/gkemulticloud",
        sum = "h1:8I84Q4vl02rJRsFiinBxl7WCozfdLlUVBQuSrqr9Wtk=",
        version = "v0.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_gsuiteaddons",
        importpath = "cloud.google.com/go/gsuiteaddons",
        sum = "h1:1mvhXqJzV0Vg5Fa95QwckljODJJfDFXV4pn+iL50zzA=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        importpath = "cloud.google.com/go/iam",
        sum = "h1:+CmB+K0J/33d0zSQ9SlFWUeCCEn5XJA0ZMZ3pHE9u8k=",
        version = "v0.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_iap",
        importpath = "cloud.google.com/go/iap",
        sum = "h1:PxVHFuMxmSZyfntKXHXhd8bo82WJ+LcATenq7HLdVnU=",
        version = "v1.7.1",
    )
    go_repository(
        name = "com_google_cloud_go_ids",
        importpath = "cloud.google.com/go/ids",
        sum = "h1:fodnCDtOXuMmS8LTC2y3h8t24U8F3eKWfhi+3LY6Qf0=",
        version = "v1.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_iot",
        importpath = "cloud.google.com/go/iot",
        sum = "h1:39W5BFSarRNZfVG0eXI5LYux+OVQT8GkgpHCnrZL2vM=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        importpath = "cloud.google.com/go/kms",
        sum = "h1:7hm1bRqGCA1GBRQUrp831TwJ9TWhP+tvLuP497CQS2g=",
        version = "v1.10.1",
    )
    go_repository(
        name = "com_google_cloud_go_language",
        importpath = "cloud.google.com/go/language",
        sum = "h1:7Ulo2mDk9huBoBi8zCE3ONOoBrL6UXfAI71CLQ9GEIM=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_lifesciences",
        importpath = "cloud.google.com/go/lifesciences",
        sum = "h1:uWrMjWTsGjLZpCTWEAzYvyXj+7fhiZST45u9AgasasI=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_logging",
        importpath = "cloud.google.com/go/logging",
        sum = "h1:CJYxlNNNNAMkHp9em/YEXcfJg+rPDg7YfwoRpMU+t5I=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_longrunning",
        importpath = "cloud.google.com/go/longrunning",
        sum = "h1:v+yFJOfKC3yZdY6ZUI933pIYdhyhV8S3NpWrXWmg7jM=",
        version = "v0.4.1",
    )
    go_repository(
        name = "com_google_cloud_go_managedidentities",
        importpath = "cloud.google.com/go/managedidentities",
        sum = "h1:ZRQ4k21/jAhrHBVKl/AY7SjgzeJwG1iZa+mJ82P+VNg=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_maps",
        importpath = "cloud.google.com/go/maps",
        sum = "h1:mv9YaczD4oZBZkM5XJl6fXQ984IkJNHPwkc8MUsdkBo=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_mediatranslation",
        importpath = "cloud.google.com/go/mediatranslation",
        sum = "h1:anPxH+/WWt8Yc3EdoEJhPMBRF7EhIdz426A+tuoA0OU=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_memcache",
        importpath = "cloud.google.com/go/memcache",
        sum = "h1:8/VEmWCpnETCrBwS3z4MhT+tIdKgR1Z4Tr2tvYH32rg=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_metastore",
        importpath = "cloud.google.com/go/metastore",
        sum = "h1:QCFhZVe2289KDBQ7WxaHV2rAmPrmRAdLC6gbjUd3HPo=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_monitoring",
        importpath = "cloud.google.com/go/monitoring",
        sum = "h1:2qsrgXGVoRXpP7otZ14eE1I568zAa92sJSDPyOJvwjM=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_networkconnectivity",
        importpath = "cloud.google.com/go/networkconnectivity",
        sum = "h1:ZD6b4Pk1jEtp/cx9nx0ZYcL3BKqDa+KixNDZ6Bjs1B8=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_networkmanagement",
        importpath = "cloud.google.com/go/networkmanagement",
        sum = "h1:8KWEUNGcpSX9WwZXq7FtciuNGPdPdPN/ruDm769yAEM=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_networksecurity",
        importpath = "cloud.google.com/go/networksecurity",
        sum = "h1:sOc42Ig1K2LiKlzG71GUVloeSJ0J3mffEBYmvu+P0eo=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_notebooks",
        importpath = "cloud.google.com/go/notebooks",
        sum = "h1:Kg2K3K7CbSXYJHZ1aGQpf1xi5x2GUvQWf2sFVuiZh8M=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_optimization",
        importpath = "cloud.google.com/go/optimization",
        sum = "h1:dj8O4VOJRB4CUwZXdmwNViH1OtI0WtWL867/lnYH248=",
        version = "v1.3.1",
    )
    go_repository(
        name = "com_google_cloud_go_orchestration",
        importpath = "cloud.google.com/go/orchestration",
        sum = "h1:Vw+CEXo8M/FZ1rb4EjcLv0gJqqw89b7+g+C/EmniTb8=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_orgpolicy",
        importpath = "cloud.google.com/go/orgpolicy",
        sum = "h1:XDriMWug7sd0kYT1QKofRpRHzjad0bK8Q8uA9q+XrU4=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_osconfig",
        importpath = "cloud.google.com/go/osconfig",
        sum = "h1:PkSQx4OHit5xz2bNyr11KGcaFccL5oqglFPdTboyqwQ=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_oslogin",
        importpath = "cloud.google.com/go/oslogin",
        sum = "h1:whP7vhpmc+ufZa90eVpkfbgzJRK/Xomjz+XCD4aGwWw=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_phishingprotection",
        importpath = "cloud.google.com/go/phishingprotection",
        sum = "h1:l6tDkT7qAEV49MNEJkEJTB6vOO/onbSOcNtAT09HPuA=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_policytroubleshooter",
        importpath = "cloud.google.com/go/policytroubleshooter",
        sum = "h1:yKAGC4p9O61ttZUswaq9GAn1SZnEzTd0vUYXD7ZBT7Y=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_privatecatalog",
        importpath = "cloud.google.com/go/privatecatalog",
        sum = "h1:EPEJ1DpEGXLDnmc7mnCAqFmkwUJbIsaLAiLHVOkkwtc=",
        version = "v0.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        importpath = "cloud.google.com/go/pubsub",
        sum = "h1:vCge8m7aUKBJYOgrZp7EsNDf6QMd2CAlXZqWTn3yq6s=",
        version = "v1.30.0",
    )
    go_repository(
        name = "com_google_cloud_go_pubsublite",
        importpath = "cloud.google.com/go/pubsublite",
        sum = "h1:cb9fsrtpINtETHiJ3ECeaVzrfIVhcGjhhJEjybHXHao=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_recaptchaenterprise_v2",
        importpath = "cloud.google.com/go/recaptchaenterprise/v2",
        sum = "h1:6iOCujSNJ0YS7oNymI64hXsjGq60T4FK1zdLugxbzvU=",
        version = "v2.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_recommendationengine",
        importpath = "cloud.google.com/go/recommendationengine",
        sum = "h1:VibRFCwWXrFebEWKHfZAt2kta6pS7Tlimsnms0fjv7k=",
        version = "v0.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_recommender",
        importpath = "cloud.google.com/go/recommender",
        sum = "h1:ZnFRY5R6zOVk2IDS1Jbv5Bw+DExCI5rFumsTnMXiu/A=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_redis",
        importpath = "cloud.google.com/go/redis",
        sum = "h1:JoAd3SkeDt3rLFAAxEvw6wV4t+8y4ZzfZcZmddqphQ8=",
        version = "v1.11.0",
    )
    go_repository(
        name = "com_google_cloud_go_resourcemanager",
        importpath = "cloud.google.com/go/resourcemanager",
        sum = "h1:NRM0p+RJkaQF9Ee9JMnUV9BQ2QBIOq/v8M+Pbv/wmCs=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_resourcesettings",
        importpath = "cloud.google.com/go/resourcesettings",
        sum = "h1:8Dua37kQt27CCWHm4h/Q1XqCF6ByD7Ouu49xg95qJzI=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_retail",
        importpath = "cloud.google.com/go/retail",
        sum = "h1:1Dda2OpFNzIb4qWgFZjYlpP7sxX3aLeypKG6A3H4Yys=",
        version = "v1.12.0",
    )
    go_repository(
        name = "com_google_cloud_go_run",
        importpath = "cloud.google.com/go/run",
        sum = "h1:ydJQo+k+MShYnBfhaRHSZYeD/SQKZzZLAROyfpeD9zw=",
        version = "v0.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_scheduler",
        importpath = "cloud.google.com/go/scheduler",
        sum = "h1:NpQAHtx3sulByTLe2dMwWmah8PWgeoieFPpJpArwFV0=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_secretmanager",
        importpath = "cloud.google.com/go/secretmanager",
        sum = "h1:pu03bha7ukxF8otyPKTFdDz+rr9sE3YauS5PliDXK60=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_security",
        importpath = "cloud.google.com/go/security",
        sum = "h1:PYvDxopRQBfYAXKAuDpFCKBvDOWPWzp9k/H5nB3ud3o=",
        version = "v1.13.0",
    )
    go_repository(
        name = "com_google_cloud_go_securitycenter",
        importpath = "cloud.google.com/go/securitycenter",
        sum = "h1:AF3c2s3awNTMoBtMX3oCUoOMmGlYxGOeuXSYHNBkf14=",
        version = "v1.19.0",
    )

    go_repository(
        name = "com_google_cloud_go_servicedirectory",
        importpath = "cloud.google.com/go/servicedirectory",
        sum = "h1:SJwk0XX2e26o25ObYUORXx6torSFiYgsGkWSkZgkoSU=",
        version = "v1.9.0",
    )

    go_repository(
        name = "com_google_cloud_go_shell",
        importpath = "cloud.google.com/go/shell",
        sum = "h1:wT0Uw7ib7+AgZST9eCDygwTJn4+bHMDtZo5fh7kGWDU=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_spanner",
        importpath = "cloud.google.com/go/spanner",
        sum = "h1:7VdjZ8zj4sHbDw55atp5dfY6kn1j9sam9DRNpPQhqR4=",
        version = "v1.45.0",
    )
    go_repository(
        name = "com_google_cloud_go_speech",
        importpath = "cloud.google.com/go/speech",
        sum = "h1:JEVoWGNnTF128kNty7T4aG4eqv2z86yiMJPT9Zjp+iw=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        importpath = "cloud.google.com/go/storage",
        sum = "h1:uOdMxAs8HExqBlnLtnQyP0YkvbiDpdGShGKtx6U/oNM=",
        version = "v1.30.1",
    )

    go_repository(
        name = "com_google_cloud_go_storagetransfer",
        importpath = "cloud.google.com/go/storagetransfer",
        sum = "h1:5T+PM+3ECU3EY2y9Brv0Sf3oka8pKmsCfpQ07+91G9o=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_talent",
        importpath = "cloud.google.com/go/talent",
        sum = "h1:nI9sVZPjMKiO2q3Uu0KhTDVov3Xrlpt63fghP9XjyEM=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_texttospeech",
        importpath = "cloud.google.com/go/texttospeech",
        sum = "h1:H4g1ULStsbVtalbZGktyzXzw6jP26RjVGYx9RaYjBzc=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_tpu",
        importpath = "cloud.google.com/go/tpu",
        sum = "h1:/34T6CbSi+kTv5E19Q9zbU/ix8IviInZpzwz3rsFE+A=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_trace",
        importpath = "cloud.google.com/go/trace",
        sum = "h1:olxC0QHC59zgJVALtgqfD9tGk0lfeCP5/AGXL3Px/no=",
        version = "v1.9.0",
    )
    go_repository(
        name = "com_google_cloud_go_translate",
        importpath = "cloud.google.com/go/translate",
        sum = "h1:GvLP4oQ4uPdChBmBaUSa/SaZxCdyWELtlAaKzpHsXdA=",
        version = "v1.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_video",
        importpath = "cloud.google.com/go/video",
        sum = "h1:upIbnGI0ZgACm58HPjAeBMleW3sl5cT84AbYQ8PWOgM=",
        version = "v1.15.0",
    )
    go_repository(
        name = "com_google_cloud_go_videointelligence",
        importpath = "cloud.google.com/go/videointelligence",
        sum = "h1:Uh5BdoET8XXqXX2uXIahGb+wTKbLkGH7s4GXR58RrG8=",
        version = "v1.10.0",
    )
    go_repository(
        name = "com_google_cloud_go_vision_v2",
        importpath = "cloud.google.com/go/vision/v2",
        sum = "h1:8C8RXUJoflCI4yVdqhTy9tRyygSHmp60aP363z23HKg=",
        version = "v2.7.0",
    )
    go_repository(
        name = "com_google_cloud_go_vmmigration",
        importpath = "cloud.google.com/go/vmmigration",
        sum = "h1:Azs5WKtfOC8pxvkyrDvt7J0/4DYBch0cVbuFfCCFt5k=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_vmwareengine",
        importpath = "cloud.google.com/go/vmwareengine",
        sum = "h1:b0NBu7S294l0gmtrT0nOJneMYgZapr5x9tVWvgDoVEM=",
        version = "v0.3.0",
    )
    go_repository(
        name = "com_google_cloud_go_vpcaccess",
        importpath = "cloud.google.com/go/vpcaccess",
        sum = "h1:FOe6CuiQD3BhHJWt7E8QlbBcaIzVRddupwJlp7eqmn4=",
        version = "v1.6.0",
    )
    go_repository(
        name = "com_google_cloud_go_webrisk",
        importpath = "cloud.google.com/go/webrisk",
        sum = "h1:IY+L2+UwxcVm2zayMAtBhZleecdIFLiC+QJMzgb0kT0=",
        version = "v1.8.0",
    )
    go_repository(
        name = "com_google_cloud_go_websecurityscanner",
        importpath = "cloud.google.com/go/websecurityscanner",
        sum = "h1:AHC1xmaNMOZtNqxI9Rmm87IJEyPaRkOxeI0gpAacXGk=",
        version = "v1.5.0",
    )
    go_repository(
        name = "com_google_cloud_go_workflows",
        importpath = "cloud.google.com/go/workflows",
        sum = "h1:FfGp9w0cYnaKZJhUOMqCOJCYT/WlvYBfTQhFWV3sRKI=",
        version = "v1.10.0",
    )
    go_repository(
        name = "in_gopkg_check_v1",
        importpath = "gopkg.in/check.v1",
        sum = "h1:Hei/4ADfdWqJk1ZMxUNpqntNwaWcugrBjAiHlqqRiVk=",
        version = "v1.0.0-20201130134442-10cb98267c6c",
    )
    go_repository(
        name = "in_gopkg_fsnotify_v1",
        importpath = "gopkg.in/fsnotify.v1",
        sum = "h1:xOHLXZwVvI9hhs+cLKq5+I5onOuwQLhQwiu63xxlHs4=",
        version = "v1.4.7",
    )
    go_repository(
        name = "in_gopkg_tomb_v1",
        importpath = "gopkg.in/tomb.v1",
        sum = "h1:uRGJdciOHaEIrze2W8Q3AKkepLTh2hOroT7a+7czfdQ=",
        version = "v1.0.0-20141024135613-dd632973f1e7",
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        importpath = "gopkg.in/yaml.v2",
        sum = "h1:obN1ZagJSUGI0Ek/LBmuj4SNLPfIny3KsKFopxRdj10=",
        version = "v2.2.8",
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        importpath = "gopkg.in/yaml.v3",
        sum = "h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA=",
        version = "v3.0.1",
    )
    go_repository(
        name = "io_opencensus_go",
        importpath = "go.opencensus.io",
        sum = "h1:y73uSU6J157QMP2kn2r30vwW1A2W2WFwSCGnAVxeaD0=",
        version = "v0.24.0",
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        importpath = "go.opentelemetry.io/proto/otlp",
        sum = "h1:rwOQPCuKAKmwGKq2aVNnYIibI6wnV7EvzgfTCzcdGg8=",
        version = "v0.7.0",
    )
    go_repository(
        name = "org_golang_google_api",
        importpath = "google.golang.org/api",
        sum = "h1:v7rj0vA0imM3Ou81k1eyFxQNScLzn71EyGnJDr+V/XI=",
        version = "v0.127.0",
    )

    go_repository(
        name = "org_golang_google_appengine",
        importpath = "google.golang.org/appengine",
        sum = "h1:FZR1q0exgwxzPzp/aF+VccGrSfxfPpkBqjIIEq3ru6c=",
        version = "v1.6.7",
    )
    go_repository(
        name = "org_golang_google_genproto",
        importpath = "google.golang.org/genproto",
        sum = "h1:8DyZCyvI8mE1IdLy/60bS+52xfymkE72wv1asokgtao=",
        version = "v0.0.0-20230530153820-e85fd2cbaebc",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_api",
        importpath = "google.golang.org/genproto/googleapis/api",
        sum = "h1:kVKPf/IiYSBWEWtkIn6wZXwWGCnLKcC8oWfZvXjsGnM=",
        version = "v0.0.0-20230530153820-e85fd2cbaebc",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_bytestream",
        importpath = "google.golang.org/genproto/googleapis/bytestream",
        sum = "h1:g3hIDl0jRNd9PPTs2uBzYuaD5mQuwOkZY0vSc0LR32o=",
        version = "v0.0.0-20230530153820-e85fd2cbaebc",
    )
    go_repository(
        name = "org_golang_google_genproto_googleapis_rpc",
        importpath = "google.golang.org/genproto/googleapis/rpc",
        sum = "h1:XSJ8Vk1SWuNr8S18z1NZSziL0CPIXLCCMDOEFtHBOFc=",
        version = "v0.0.0-20230530153820-e85fd2cbaebc",
    )

    go_repository(
        name = "org_golang_google_grpc",
        importpath = "google.golang.org/grpc",
        sum = "h1:3Oj82/tFSCeUrRTg/5E/7d/W5A1tj6Ky1ABAuZuv5ag=",
        version = "v1.55.0",
    )
    go_repository(
        name = "org_golang_google_protobuf",
        importpath = "google.golang.org/protobuf",
        sum = "h1:kPPoIgf3TsEvrm0PFe15JQ+570QVxYzEvvHqChK+cng=",
        version = "v1.30.0",
    )
    go_repository(
        name = "org_golang_x_crypto",
        importpath = "golang.org/x/crypto",
        sum = "h1:LF6fAI+IutBocDJ2OT0Q1g8plpYljMZ4+lty+dsqw3g=",
        version = "v0.9.0",
    )
    go_repository(
        name = "org_golang_x_exp",
        importpath = "golang.org/x/exp",
        sum = "h1:c2HOrn5iMezYjSlGPncknSEr/8x5LELb/ilJbXi9DEA=",
        version = "v0.0.0-20190121172915-509febef88a4",
    )
    go_repository(
        name = "org_golang_x_lint",
        importpath = "golang.org/x/lint",
        sum = "h1:XQyxROzUlZH+WIQwySDgnISgOivlhjIEwaQaJEJrrN0=",
        version = "v0.0.0-20190313153728-d0100b6bd8b3",
    )
    go_repository(
        name = "org_golang_x_mod",
        importpath = "golang.org/x/mod",
        sum = "h1:LUYupSeNrTNCGzR/hVBk2NHZO4hXcVaW1k4Qx7rjPx8=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        sum = "h1:X2//UzNDwYmtCLn7To6G58Wr6f5ahEAQgKNzv9Y951M=",
        version = "v0.10.0",
    )
    go_repository(
        name = "org_golang_x_oauth2",
        importpath = "golang.org/x/oauth2",
        sum = "h1:6dkIjl3j3LtZ/O3sTgZTMsLKSftL/B8Zgq4huOIIUu8=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_sync",
        importpath = "golang.org/x/sync",
        sum = "h1:PUR+T4wwASmuSTYdKjYHI5TD22Wy5ogLU5qZCOLxBrI=",
        version = "v0.2.0",
    )
    go_repository(
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        sum = "h1:EBmGv8NaZBZTWvrbjNoL6HVt+IVy3QDQpJs7VRIw3tU=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_term",
        importpath = "golang.org/x/term",
        sum = "h1:n5xxQn2i3PC0yLAbjTpNT85q/Kgzcr2gIoX9OrJUols=",
        version = "v0.8.0",
    )
    go_repository(
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        sum = "h1:2sjJmO8cDvYveuX97RDLsxlyUxLl+GHoLxBiRdHllBE=",
        version = "v0.9.0",
    )
    go_repository(
        name = "org_golang_x_tools",
        importpath = "golang.org/x/tools",
        sum = "h1:BOw41kyTf3PuCW1pVQf8+Cyg8pMlkYB1oo9iJ6D/lKM=",
        version = "v0.6.0",
    )
    go_repository(
        name = "org_golang_x_xerrors",
        importpath = "golang.org/x/xerrors",
        sum = "h1:H2TDz8ibqkAF6YGhCdN3jS9O0/s90v0rJh3X/OLHEUk=",
        version = "v0.0.0-20220907171357-04be3eba64a2",
    )
    go_repository(
        name = "org_uber_go_atomic",
        importpath = "go.uber.org/atomic",
        sum = "h1:ADUqmZGgLDDfbSL9ZmPxKTybcoEYHgpYfELNoN+7hsw=",
        version = "v1.7.0",
    )
    go_repository(
        name = "org_uber_go_goleak",
        importpath = "go.uber.org/goleak",
        sum = "h1:wy28qYRKZgnJTxGxvye5/wgWr1EKjmUDGYox5mGlRlI=",
        version = "v1.1.11",
    )
    go_repository(
        name = "org_uber_go_multierr",
        importpath = "go.uber.org/multierr",
        sum = "h1:y6IPFStTAIT5Ytl7/XYmHvzXQ7S3g/IeZW9hyZ5thw4=",
        version = "v1.6.0",
    )
    go_repository(
        name = "org_uber_go_zap",
        importpath = "go.uber.org/zap",
        sum = "h1:FiJd5l1UOLj0wCgbSE0rwwXHzEdAZS6hiiSnxJN/D60=",
        version = "v1.24.0",
    )
