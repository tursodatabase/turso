// use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
// use crate::{bail_parse_error, Result, SymbolTable};

// pub fn translate_destroy(query_mode: QueryMode) -> Result<ProgramBuilder> {
//     let mut program = ProgramBuilder::new(ProgramBuilderOpts {
//         query_mode,
//         num_cursors: 1,
//         approx_num_insns: 30,
//         approx_num_labels: 5,
//     });

//     Ok(program)
// }
