(defproject bbakersmith/simple-batch-processor "1.1.0"
  :description "A simple Clojure stream to batch processor."
  :url "https://github.com/bbakersmith/simple-batch-processor"
  :license {:name "Apache License"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.7.0-RC2"]
                 [com.climate/claypoole "1.1.2"]]
  :deploy-repositories [["releases" :clojars]]
  :profiles {:dev {:dependencies [[speclj "3.3.1"]]}
             :user {:signing {:gpg-key "bbakersmith@gmail.com"}}}
  :plugins [[speclj "3.3.1"]]
  :test-paths ["spec"])
