## Laboratorio 1

### Corrigió: Damián

### Nota: 5 (65.34 %)

### Comentarios

* No cachea la entrada raw (se usa despues muchas veces)
* En `aggregateByMonth` podrian haber usado mejor el metodo `toString` del un objeto LocalDate. Este metodo puede recibir una especificacion de formato, por lo cual podrian haber escrito: p._1.toString('yyyy-MM').
* No está mapa de geolocalización
* No me carga la definición de getMetadata. 
  ```
  error: bad symbolic reference. A signature in DefaultReads.class refers to term time
  in package java which is not available.
  It may be completely missing from the current classpath, or the version on
  the classpath might be incompatible with the version used when compiling DefaultReads.class.
                   (metadata \ "title").asOpt[String].getOrElse(""),
  ```

  Supongo que es un problema de compatibilidad de Java 7 y 8


## Laboratorio 2

### Corrigió: Ezequiel

### Nota: 10 (99.18 %)

### Comentarios:

Excelente trabajo!

#### JOB:
- Cuando computan `hitsDataset` el `split('\t')` se podría haber hecho una sola vez.

- Porque no guardan el timestamp como `Timestamp` en el archivo `HitsDataset.parquet`? Al guardarlo como string fuerzan al usuario del dataset a  tener que hacer el casteo a timestamp cuando lo quiera usar.


