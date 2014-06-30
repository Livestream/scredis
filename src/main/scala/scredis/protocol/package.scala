package scredis

package object protocol {
  type Decoder[X] = PartialFunction[Response, X]
}