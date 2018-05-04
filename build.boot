(def project 'clog)
(def version "0.1.0-SNAPSHOT")

(set-env! :resource-paths #{"resources" "src"}
          :source-paths   #{"test"}
          :dependencies   '[[org.clojure/clojure "1.9.0"]
                            [org.clojure/data.csv "0.1.4"]
                            [org.clojure/tools.cli "0.3.7"]
                            [com.cognitect/transit-clj "0.8.309"]
                            [datascript "0.16.4"]
                            [adzerk/boot-test "RELEASE" :scope "test"]])

(task-options!
 aot {:namespace   #{'clog.core}}
 pom {:project     project
      :version     version
      :description "FIXME: write description"
      :url         "http://example/FIXME"
      :scm         {:url "https://github.com/yourname/clog"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}}
 repl {:init-ns    'clog.core}
 jar {:main        'clog.core
      :file        (str "clog-" version "-standalone.jar")})

(deftask compile-all-aot
  "compile all namespaces aot"
  [d dir PATH #{str} "the set of directories to write to (target)."]
  (let [dir (if (seq dir) dir #{"target"})]
    (comp (aot :all true)
          (pom)
          (uber)
          (target :dir dir))))

(deftask build-native-image-graalvm
  "Build a native image from existing AOT classes"
  []
  (with-pass-thru fs
    (println "Building native image")
    (println (:out (clojure.java.shell/sh
                    "native-image"
                    "-H:+ReportUnsupportedElementsAtRuntime"
                    "-H:Name=clog"
                    "-cp" ".:target"
                    "clog.core")))))

(deftask build-native-image
  "AOT compile namespaces and then build image with GraalVM's `native-imamge`"
  [d dir PATH #{str} "the set of directories to write to (target)."]
  (binding [clojure.core/*compiler-options* {:direct-linking true}]
    (comp (compile-all-aot :dir dir)
          (build-native-image-graalvm))))

(deftask build
  "Build the project locally as a JAR."
  [d dir PATH #{str} "the set of directories to write to (target)."]
  (let [dir (if (seq dir) dir #{"target"})]
    (comp (aot) (pom) (uber) (jar) (target :dir dir))))

(deftask run
  "Run the project."
  [a args ARG [str] "the arguments for the application."]
  (with-pass-thru fs
    (require '[clog.core :as app])
    (apply (resolve 'app/-main) args)))

(require '[adzerk.boot-test :refer [test]])
