package com.snowplowanalytics.iglu.schemaddl


import collection.immutable.ListMap
import cats.data.{NonEmptyList, State}
import io.circe.Json
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.schemaddl.VersionTree.VersionList
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{JsonPointer, Schema}

import scala.annotation.tailrec

/**
  *
  * @param subschemas (order should not matter at this point)
  * @param required
  */
case class FlatSchema(subschemas: List[(JsonPointer, Schema)], required: Set[JsonPointer]) {
  def add(pointer: JsonPointer, schema: Schema): FlatSchema =
    FlatSchema((pointer, schema) :: subschemas, required)

  def withRequired(pointer: JsonPointer, schema: Schema): FlatSchema = {
    val currentRequired = FlatSchema
      .getRequired(pointer, schema)
      .filter(pointer => FlatSchema.nestedRequired(required, pointer))
    this.copy(required = currentRequired ++ required)
  }

  def toMap: ListMap[String, Schema] = ListMap(subschemas: _*).map { case (k, v) => (k.show, v) }
}


object FlatSchema {
  def flatten(json: SelfDescribingData[Json]): Either[String, List[String]] = ???

  /** Schema group metadata extracted from repository */
  case class SchemaGroupMeta(vendor: String, name: String, versions: VersionList)
  /** Full schemas */
  case class SchemaGroup private(meta: SchemaGroupMeta, schemas: NonEmptyList[Schema])

  case class Changed(what: String, from: Schema.Primitive, to: Schema)
  case class Diff(added: (String, Schema), removed: List[String], changed: Changed)

  def diff(first: Schema, next: Schema): Diff = {
    ???
  }

  def flatten2(data: Json, schemas: SchemaGroup): Either[String, List[String]] = ???

  def build(schema: Schema): FlatSchema =
    Schema.traverse(schema, FlatSchema.save).runS(FlatSchema.empty).value

  /** Check if `current` JSON Pointer has all parent elements also required */
  // TODO: consider that pointer can be nullable
  @tailrec def nestedRequired(known: Set[JsonPointer], current: JsonPointer): Boolean =
    current.parent.flatMap(_.parent) match {
      case None | Some(JsonPointer.Root) => true  // Technically None should not be reached
      case Some(parent) => known.contains(parent) && nestedRequired(known, parent)
    }

  /** Redshift-specific */
  // TODO: type object with properties can be primitive if properties are empty
  def isPrimitive(schema: Schema): Boolean = {
    val isNested = schema.withType(CommonProperties.Type.Object) && schema.properties.isDefined
    !isNested
  }

  /** This property shouldn't have been added (FlattenerSpec.e10) */
  def hasPrimitiveParent(pointer: JsonPointer): Boolean = {
    pointer.value.exists {
      case JsonPointer.Cursor.DownProperty(JsonPointer.SchemaProperty.Items) => true
      case _ => false
    }
  }

  def getRequired(cur: JsonPointer, schema: Schema): Set[JsonPointer] =
    schema
      .required.map(_.value.toSet)
      .getOrElse(Set.empty)
      .map(prop => cur.downProperty(JsonPointer.SchemaProperty.Properties).downField(prop))

  val empty = FlatSchema(Nil, Set.empty)

  def save(pointer: JsonPointer, schema: Schema): State[FlatSchema, Unit] =
    State.modify[FlatSchema] { schemaTypes =>
      if (hasPrimitiveParent(pointer)) schemaTypes
      else if (isPrimitive(schema)) schemaTypes.add(pointer, schema).withRequired(pointer, schema)
      else schemaTypes.withRequired(pointer, schema)
    }
}