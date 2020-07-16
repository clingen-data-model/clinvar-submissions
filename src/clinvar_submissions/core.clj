(ns clinvar-submissions.core
  (:require [clinvar-submissions.db :as db]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log])
  (:gen-class))

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     (System/getenv "KAFKA_USER")
                 :kafka-password (System/getenv "KAFKA_PASSWORD")
                 :kafka-consumer-topic    "clinvar-raw-test"})

(def topic-metadata
  {:input
   {:topic-name "clinvar-raw"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}
   :output
   {:topic-name "clinvar-scv"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}})

(defn kafka-config
  "Expects, at a minimum, :kafka-user and :kafka-password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "application.id"                        "clinvar-submissions-local"
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})


(defn select-clinical-assertion
  "Return true if the message is a clinical assertion
  otherwise return nil"
  [[key v]]
  (log/trace "in select-clinical-assertion " key)
  (let [record (json/parse-string v true)]
    (= "clinical_assertion" (get-in record [:content :entity_type]))))

(defonce rocksdb (db/init! ))


(defn topology [in-topic out-topic]
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder in-topic)
        ;; Stash data in RocksDB
        ;;(j/transform-values processor-fn)
        (j/filter select-clinical-assertion)
        ;; Xform clinical assertion using stored data
        (j/to out-topic))
    builder))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/set-level! :info)
  (let [builder (topology (:input topic-metadata) (:output topic-metadata))
        app (j/kafka-streams builder (kafka-config app-config))]
    (j/start app)))



