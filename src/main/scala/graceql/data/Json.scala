package graceql.data

import scala.util.{Try, Success}

case class Json private(underlying: String) extends AnyVal
object Json:
    def parse(value: String): Try[Json] = Success(Json(value))
