(ns main.core(
               :require
               [main.normalizer-functions :as func]
               [main.enums :as enums]))

; cria variáveis globais
(def example1 (func/read-csv "csv-examples/example1.csv"))
(def example2 (func/read-csv "csv-examples/example2.csv"))
(def example3 (func/read-csv "csv-examples/example3.csv"))

; Printa o csv
(defn print-csv
  [csv]
  (doseq [row csv]
    (println row))
  )

;; Cria uma chave primária numérica de 1 a n linhas
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-index "idCrescente"))
;  )

;; Cria uma chave primária numérica de n linhas a 1
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-index-descending "idCrescente"))
;  )
;
;; Cria uma chave primária com letras maiúculas
;(defn -main
;  []
;  (print-csv (func/create-index example1 enums/sequential-letters "idCrescente"))
;  )
;
; Cria uma tabela auxiliar para evitar repetições
;(defn -main
;  []
;  (let [tables (func/replace-columns-with-foreign-keys example1 [2 3])]
;    (print-csv (first tables))
;    (println)
;    (print-csv (second tables)))
;  )
;
; Remove linhas duplicadas
;(defn -main
;  []
;  (print-csv (func/remove-duplicate-rows example2))
;  )
;
; Remove atributos multivalorados
(defn -main
  []
  (print-csv (func/split-multivalued-column example3 1 ";" ["Hobby1" "Hobby2" "Hobby3"]))
  )
;
; Cria o arquivo csv
;(defn -main
;  []
;  (func/write-csv (func/remove-duplicate-rows example2) "/home/rhuan/Documents/teste.csv")
;  )