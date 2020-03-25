# Project: `examples`

## Prerequisites
* Scala
* SBT
* Dependent libraries
  * `Zeta` jars (for example `2.8_stable`)

## Build procedure
* build desired version of `zeta` and publish artifacts to local `ivy` repository
  * For example: `zeta> JAVA_OPTS="-Dversion=2.8_stable" ./sbt clean assembly publishLocal`
* build `examples` project with specific version of `Zeta`:
  * For example: `examples> VERSION=2.8_stable ./sbt clean build`
  
## Archiving
* `zip -r examples.zip examples -x "*.idea*" -x "*target*"`
