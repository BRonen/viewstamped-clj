(ns bronen.experimental
  (:import [java.util.concurrent Executors]))

(def vthread-executor
  (Executors/newVirtualThreadPerTaskExecutor))

(set-agent-send-off-executor! vthread-executor)

(defn error-handler!
  [a ex]
  (println "Actor failed" a ex)
  123)

(defn gen-agents
  [n]
  (let [actors (for [a (range n)]
                 ;; TODO better agents addressing
                 [(str a) (agent {:name    (str a)
                                  :counter 0
                                  :log     []})])
        _      (doseq [[_ actor] actors]
                 (set-error-handler! actor error-handler!))]
    (apply conj {} actors)))

(def agents (gen-agents 3))

(defn receive!
  "Accepts the current current state, a message and a continuation.
  It's not required to call the continuation, but it will be considered
  as an error.
  Also, needs to return the next state of the agent."
  [self {:keys [message send-response]}]
  #_(throw (Exception. "test"))
  (Thread/sleep 1000)
  (prn (str "agent["(:name self)"] received :> " message))
  (send-response {:type :ok})
  (-> self
      (update :counter inc)
      (update :log conj (str "agent["(:name self)"] received :> " message))))

#_(send (get agents "0") receive! {:message "dwakjhwadhkwhatevwajdkkadjhwher."})

(defn cast!
  "Dispatches a `receive!` inside the `to-address` agent, passing
  the `message` but timeouts in `timeout-ms` if the agents takes
  too long to respond."
  [{:keys [to-address message timeout-ms]
    :or   {timeout-ms 1000}}]
  (let [response (promise)
        to-agent (get agents to-address)]

    (send-off to-agent receive! {:message       message
                                 :send-response (partial deliver response)})

    (deref response timeout-ms {:type  :error
                                :error :timeout})))


;; (def registry (atom {}))

;; (defn spawn-actor
;;   [initial-state]
;;   (let [pid   (gensym "actor-")
;;         state (assoc initial-state :pid pid)
;;         actor (agent state)]
;;     (set-error-handler! actor error-handler!)
;;     (swap! registry assoc pid actor)
;;     pid))

#_(time
    (doseq [n (range 10)]
      (cast! {:to-address (str (mod n 3))
              :message    (str "hellow world - " n)})))

#_(-> (get agents "1") agent-error .getMessage)
#_(mapv deref (vals agents))
