(ns clinvar-submissions.core-test
  (:require [clojure.test :refer :all]
            [clinvar-submissions.core :refer :all]))


(deftest test-clinical-assertion-pred
  (testing "Check clinical assertion predicate for correctness"
    (let [v "{\"time\":\"2020-01-01T12:00:00Z\",\"type\":\"create\",\"content\":{\"variation_id\":\"634922\",\"variation_archive_id\":\"VCV000634922\",\"submitter_id\":\"3\",\"date_last_updated\":\"2019-06-24\",\"interpretation_comments\":[],\"interpretation_description\":\"risk factor\",\"trait_set_id\":\"47782\",\"internal_id\":\"1807657\",\"entity_type\":\"clinical_assertion\",\"submission_id\":\"3.2019-06-18\",\"local_key\":\"114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"clinical_assertion_observation_ids\":[\"SCV000924344.0\"],\"title\":\"NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"assertion_type\":\"variation to disease\",\"rcv_accession_id\":\"RCV000785779\",\"clinical_assertion_trait_set_id\":\"SCV000924344\",\"id\":\"SCV000924344\",\"submission_names\":[],\"record_status\":\"current\",\"date_created\":\"2019-06-20\",\"review_status\":\"no assertion criteria provided\",\"interpretation_date_last_evaluated\":\"2019-06-18\",\"version\":\"1\"}}"]
      (is (select-clinical-assertion nil v)))))
