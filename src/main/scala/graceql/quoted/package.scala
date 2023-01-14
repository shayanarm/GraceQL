package graceql

package object quoted {
  transparent inline def memoize(inline e: Any) : Any = 
    ${Memoize.memoize('e)}
}
