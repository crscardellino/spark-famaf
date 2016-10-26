# Laboratorio 2: Web Analytics

En este laboratorio trabajaremos sobre web analytics y en particular sobre una tarea de gran importancia en dicha área: **sessionization**.

La tarea de sessionization consiste en reconstruir las **sesiones** de los usuarios a partir de un log de actividades.

Una sesión es un grupo de interacciones que se realizan en un sitio web dentro de un periodo de tiempo. Por ejemplo, una sesión puede contener multiples visualizaciones de una página, interacciones con diversos elementos en la misma, transacciones comerciales (checkouts) etc [1].

El concepto de sesión es muy importante en analytics ya que muchas métricas y reportes dependen de como se calculan las sesiones.

El objetivo de este laboratorio será el desarrollo de un sessionization job y de un reporte sobre diversas métricas basadas en sesiones.

## Dataset

Para este laboratorio usaremos un dataset (event-log.tar.gz) que contiene el log de eventos (hits) en un sitio de e-commerce ficticio.
Este dataset contiene 4291885 registros correspondientes a dos días de información.
Cada registro contiene el **timestamp** del evento,  el **id del usuario**, el **id del evento** y el **monto** de la operación si el evento corresponde a un checkout.

Existen 3 eventos posibles:

1. **Render** de la pagina principal (id: 12c6fc06c99a462375eeb3f43dfd832b08ca9e17)
2. **Play** en un video (id: bd307a3ec329e10a2cff8fb87480823da114f8f4).
3. **Checkout** (id: 7b52009b64fd0a2a49e6d8a939753077792b0554).

## Consignas

* **Fecha de Entrega:** 02/11

* **Formato de entrega:** Se debe comitear al repositorio del grupo el el proyecto completo del job y el notebook de Zeppelin con las métricas bajo el tag `lab-2`.
* **Restricciones:** Solo se pueden utilizar Datasets/Dataframes. 


## Sessionization Job
Implementar un job de spark que  reconstruya las sesiones de los usuarios a partir del log de actividades.

### Requerimientos funcionales:

1. Debe ser implementado como una aplicación standalone.

2. Como argumento debe recibir: 

	a. El nombre del directorio donde esta el dataset o del archivo.
	
	b. El tiempo de expiración de una sesión.

3. Debe generar dos datasets en formato **parquet**:

	a. Un dataset de sesiones con la siguiente información: id del usuario, inicio de sesión, fin de sesión, cantidad de renders, cantidad de plays, cantidad de checkouts y monto total.
		  
	b. El dataset de hits.

 **Recomendaciones:** 
	 * Las funciones de ventana (window functions)[4] pueden serle de mucha ayuda.

### Requerimientos No funcionales:
1. Debe respetarse la estructura del proyecto sugerida por Maven.

2. Debe utilizar `sbt` para gestionar dependencias y empaquetar el job.

3. Debe incluirse un README donde se indique como ejecutar el job con `spark-submit`.

## Métricas

A continuación, usando un notebook de Zeppelin compute las siguientes métricas:

1. Cantidad de usuarios distintos por día.
2. Cantidad de usuarios que retornan por día.
3. Duración promedio de una sesión.
4. Mediana de renders, plays y checkout por sesión.
5. Cantidad de visitas por hora. 
6. Valor promedio por checkout.


## Análisis de funnel (Posgrado).

1. Calcule el **conversion rate** [3].
2. En analytics, se denomina funnel (embudo)[2] a una serie de eventos que llevan hacia un objetivo determinado.
Considerando como objetivo al evento de checkout:
Compute el funnel **Render->Play->Checkout**.


 
## Referencias


**[1]** [Google Analytics](https://support.google.com/analytics/answer/2731565?hl=en)

**[2]** [Funnels](http://www.cooladata.com/blog/funnels/)

**[3]** [Conversion Rate](https://en.wikipedia.org/wiki/Conversion_marketing#Conversion_rate)

**[4]** [Windows Functions Tutorial](https://www.postgresql.org/docs/9.1/static/tutorial-window.html)

**[5]** [Windows Functions en Spark](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-windows.html)

<!--1. Computar sessiones en a partir de el log de actividades.
	1. Pasar el rdd a dataset


La api de dataset/dataframes no es muy intuitiva y uno se encuentra pasando de uno a otro tipo constantemente.

Este constante paso de una a otro tipo puede crear confusion.

Como thumb-rule: Usar dataset como reemplazo para RDDs y para hacer queries simples, principalmente de agregacion.
Si queres trabajar con una "tabla" es mas intuitivo pensar en terminos de Dataframes. El costo es perder el type-safety.--> 	