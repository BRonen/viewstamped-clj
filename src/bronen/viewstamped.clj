(ns bronen.viewstamped)

(def replicas-size 3)

(defn get-quorum-size
  [n]
  (-> n (/ 2) inc int))

#_(get-quorum-size replicas-size)

(defn get-primary!
  [replicas]
  (let [replicas->view-number         #(-> % deref (get :view-number))
        view-frequency->current-view? (fn [[view view-count]]
                                        (>= view-count
                                            (-> replicas
                                                count
                                                get-quorum-size)))

        views-numbers     (map replicas->view-number replicas)
        views-frequencies (frequencies views-numbers)
        [[view-number _]] (filter view-frequency->current-view?
                                  views-frequencies)
        primary-id        (mod view-number replicas-size)]
    (nth replicas primary-id)))

#_(get-primary! replicas)

(defn gen-replicas
  [n]
  (let [config (for [r-id (range n)
                     :let [port (+ r-id 5000)]]
                 {:port   port
                  :origin (str "localhost:" port)})]
    (mapv #(agent {:configuration  config
                   :replica-number %
                   :view-number    0
                   :status         :normal
                   :op-number      0
                   :log            []
                   :commit-number  0
                   ;; \/ {:client-id {:request-number n :result r}}
                   :client-table   {}})
          (range n))))

(def replicas (gen-replicas replicas-size))

#_(map deref replicas)

(declare cast!)
(declare broadcast!)

(defn handle-messages
  [{:keys [self message send-response] :as args}]
  (def wasd args)
  (letfn [(mrequest []
            (let [{:keys [client-id
                          request-number]} message
                  last-request-number      (-> self
                                               :client-table
                                               (get client-id)
                                               (get :request-number)
                                               (or 0))]
              (cond
                (> request-number last-request-number)
                (let [new-self    (-> self
                                      (update :op-number inc)
                                      (update :log conj message)
                                      (assoc-in [:client-table
                                                 client-id
                                                 :request-number]
                                                request-number)
                                      (update :commit-number inc))
                      quorum-size (-> self
                                      :configuration
                                      count
                                      get-quorum-size)
                      result      (broadcast! {:from-id    (:replica-number self)
                                               :message    {:type          :prepare
                                                            :view-number   (:view-number self)
                                                            :operation     message
                                                            :op-number     (:op-number self)
                                                            :commit-number (:commit-number self)}
                                               :timeout-ms 10000})]
                  (def result2 result)
                  (if (-> result :type (= :success))
                    (do (prn "executing a message from primary> " message)
                        (send-response {:type           :reply
                                        :view-number    (:view-number self)
                                        :request-number request-number
                                        :result         "TODO sqlite integration"})
                        new-self)
                    (do (send-response {:type  :fail
                                        :error "quorum unreachable"})
                        self)))

                (= request-number last-request-number)
                (do (send-response {:type     :success
                                    :response (-> self
                                                  :client-table
                                                  (get client-id)
                                                  (get :response))})
                    self)

                (< request-number last-request-number)
                (do (send-response {:type :fail})
                    self))))
          (mprepare []
            (let [{:keys [view-number
                          op-number
                          operation
                          commit-number]} message
                  next-operation?         true #_ (-> self :op-number inc (= op-number))]
              (if-not next-operation?
                (do (send-response {:type  :error
                                    :error "outdated state"})
                    ;; TODO state transfer
                    self)
                (let [self (-> self
                               #_(update :op-number inc)
                               (update :log conj operation)
                               (assoc-in [:client-table
                                          (:client-id operation)
                                          :request-number]
                                         (:request-number operation)))]
                  (send-response {:type :prepare-ok})
                  self))))
          (mexcept []
            (send-response {:type  :error
                            :error (str "replica["
                                        (:replica-number self)
                                        "] received invalid message: "
                                        (:type message))})
            self)]
    (case (:type message)
      :request (mrequest)
      :prepare (mprepare)
      (mexcept))))

(defn cast!
  "Dispatches the handler of the `to-id` replica passing the `message`."
  [{:keys [to-id message timeout-ms]
    :or   {timeout-ms 5000}}]
  (let [response (promise)
        to-agent (nth replicas to-id)]
    (letfn [(dispatcher [self]
              (try
                (handle-messages {:self          self
                                  :message       message
                                  :send-response (partial deliver response)})
                (catch Exception e
                  (deliver response nil)
                  (prn e))))]
      (send-off to-agent dispatcher))
    (deref response timeout-ms {:type  :error
                                :error :timeout})))

#_(cast! 1 {:type    :requesta
            :message "teste"})

(defn broadcast! [{:keys [from-id message timeout-ms]
                   :as   args
                   :or   {timeout-ms 5000}}]
  "Casts a `message` from `from-id` replica to all other replicas.

   Waits until a quorum of `success` or a quorum of `fail` is reached,
   and then return the responses.

  If the replicas don't respond before `timeout-ms`, an `error` is returned."
  (def wasdw2 args)
  (let [quorum-size     (-> replicas-size get-quorum-size dec)
        quorum-promise? (promise)
        responses       (atom [])]
    (letfn [(send-response [res]
              (swap! responses conj res)
              (cond
                (->> @responses
                     (filter #(-> % :type (= :prepare-ok)))
                     count
                     (< quorum-size))
                (deliver quorum-promise? :success)

                (->> @responses
                     (filter #(-> % :type (= :error)))
                     count
                     (< quorum-size))
                (deliver quorum-promise? :fail)

                :else
                nil))
            (dispatcher [self]
              (try
                (handle-messages {:self          self
                                  :message       message
                                  :send-response send-response})
                (catch Exception e
                  (deliver quorum-promise? :error)
                  (prn e))))]

      (doseq [to-agent replicas]
        (when (-> @to-agent :replica-number (not= from-id))
          (send to-agent dispatcher))))

    (release-pending-sends)

    (let [quorum-state (deref quorum-promise? timeout-ms :timeout)]
      (case quorum-state
        :success
        {:type      :success
         :responses @responses}

        :fail
        {:type      :fail
         :responses @responses}

        :timeout
        {:type      :error
         :error     :timeout
         :responses @responses}

        {:type      :error
         :error     :unexpected
         :responses @responses}))))

;; to-id message timeout-ms
#_(def temp (cast! {:to-id      0
                    :message    {:type           :request
                                 :operation      "something TODO"
                                 :client-id      1
                                 :request-number 1}
                    :timeout-ms 5000}))
#_(-> temp)
#_(ns-unmap (find-ns 'bronen.viewstamped) 'foo)
#_(mapv deref replicas)
