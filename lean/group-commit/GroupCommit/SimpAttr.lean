import Lean
import GroupCommit.Invariant

open Lean Meta in
/-- Rewrite rules for state updates of the model. -/
initialize sysSimpExt : SimpExtension ←
  registerSimpAttr `sys_simps "rewrite rules for state updates of the group commit model"
