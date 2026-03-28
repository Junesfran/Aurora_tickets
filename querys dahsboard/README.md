**CONSULTAS DASHBOARD**
---
* 1.- Total sesiones

      fields @timestamp, event, session_id
      |  stats count_distinct(session_id) as sessions by event

* 2 .-  Eventos totales a lo largo del tiempo

      fields event, event_id
        |  stats count() as total by event_id
        |  sort total desc
        |  limit 10

* 3 .-  Conteo de errores

       fields @timestamp, event
          |  filter event = "error"
          |  stats count() as total_errors

* 4 .-  Conteo por ip

       fields ip
          |  stats count() as request by ip
          |  sort requests desc
          |  limit 10
