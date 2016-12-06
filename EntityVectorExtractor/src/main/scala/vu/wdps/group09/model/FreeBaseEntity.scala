package vu.wdps.group09.model

case class FreeBaseEntity(id: String, label: String) {
  override def equals(that: Any): Boolean =
    that match {
      case that: FreeBaseEntity => this.id == that.id && this.label == that.label
      case _ => false
    }
}
