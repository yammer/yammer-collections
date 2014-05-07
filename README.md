Yammer Collections [![Build Status](https://travis-ci.org/yammer/yammer-collections.png)](https://travis-ci.org/yammer/yammer-collections)

yammer-collections
==================

Collection utilities used in Yammer, which build on top of the Guava collections library.

This package is at an early stage of the development.

To start using it, just include the following maven dependency in your pom file:

    <dependency>
      <groupId>com.yammer.collections</groupId>
      <artifactId>yammer-collections</artifactId>
      <version>0.0.4</version>
    </dependency>

Contents
--------

**com.yammer.collections.transforming** - a set of collection implementations that provide transforming live writable views of other collections.

For example, Google Guava provides a `Collections2.transform()` method which takes a collection of type *A*, and a transformation function of type *A->B*,
and returns a live view of that collection as a collection of type *B*. This view is read only as there are no requirements placed on the transformation
function. In contrast the `TransformingCollection` class takes a collection of type *A* and two transforming functions of types *A->B* and *B->A*,
with the requirement that the first function is bijective and that the second function is its reverse, and returns a read-write view of that collection as a
collection of type *B*.

