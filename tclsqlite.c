#include <tcl.h>

static int Hello_Cmd(ClientData cdata, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    Tcl_SetObjResult(interp, Tcl_NewStringObj("Hello, World!", -1));
    return TCL_OK;
}

static int Another_Cmd(ClientData cdata, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
    Tcl_SetObjResult(interp, Tcl_NewStringObj("Hello, World!", -1));
    return TCL_OK;
}

int DLLEXPORT Hello_Init(Tcl_Interp *interp) {
    if (Tcl_InitStubs(interp, "9.0", 0) == NULL) {
        return TCL_ERROR;
    }
    Tcl_CreateObjCommand(interp, "hello", Hello_Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "another", Another_Cmd, NULL, NULL);

    Tcl_PkgProvide(interp, "Hello", "1.0");
    Tcl_PkgProvide(interp, "Another", "1.0");

    return TCL_OK;
}
