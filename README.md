## stream-utils

[![Scala CI](https://github.com/mwkohout/stream-utils/actions/workflows/scala.yml/badge.svg)](https://github.com/mwkohout/stream-utils/actions/workflows/scala.yml)

### Description

This project has a number of high utility(but not core) Flows/GraphStages.

#### scaladsl.CachedFlow

Will cache the result of an operation by some calculated key inside a passed Cache object(such as a Guava Cache).
