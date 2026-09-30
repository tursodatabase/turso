# 2010 June 03
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#
# This file contains common code used by many different malloc tests
# within the test suite.
#

proc wal_file_size {nFrame pgsz} {
  expr {32 + ($pgsz+24)*$nFrame}
}

proc wal_frame_count {zFile pgsz} {
  if {[file exists $zFile]==0} { return 0 }
  set f [file size $zFile]
  if {$f < 32} { return 0 }
  expr {($f - 32) / ($pgsz+24)}
}

proc wal_cksum_intlist {ckv1 ckv2 intlist} {
  upvar $ckv1 c1
  upvar $ckv2 c2
  foreach {v1 v2} $intlist {
    set c1 [expr {($c1 + $v1 + $c2)&0xFFFFFFFF}]
    set c2 [expr {($c2 + $v2 + $c1)&0xFFFFFFFF}]
  }
}


# This proc calculates checksums in the same way as those used by SQLite 
# in WAL files. If the $endian argument is "big", then checksums are
# calculated by interpreting data as an array of big-endian integers. If
# it is "little", data is interpreted as an array of little-endian integers.
#
proc wal_cksum {endian ckv1 ckv2 blob} {
  upvar $ckv1 c1
  upvar $ckv2 c2

  if {$endian!="big" && $endian!="little"} {
    return -error "Bad value \"$endian\" - must be \"big\" or \"little\""
  }
  set scanpattern I*
  if {$endian == "little"} { set scanpattern i* }

  binary scan $blob $scanpattern values
  wal_cksum_intlist c1 c2 $values
}

proc wal_set_walhdr {filename {intlist {}}} {
  if {[llength $intlist]==6} {
    set blob [binary format I6 $intlist]
    set endian little
    if {[lindex $intlist 0] & 0x00000001} { set endian big }
    set c1 0
    set c2 0
    wal_cksum $endian c1 c2 $blob
    append blob [binary format II $c1 $c2]

    set fd [open $filename r+]
    fconfigure $fd -translation binary
    seek $fd 0
    puts -nonewline $fd $blob
    close $fd
  }

  set fd [open $filename]
  fconfigure $fd -translation binary
  set blob [read $fd 24]
  close $fd

  binary scan $blob I6 ints
  set ints
}

proc wal_fix_walindex_cksum {hdrvar} {
  upvar $hdrvar hdr
  set c1 0
  set c2 0
  wal_cksum_intlist c1 c2 [lrange $hdr 0 9]
  lset hdr 10 $c1
  lset hdr 11 $c2
}

# This command assumes that $file is the name of a database file opened
# in wal mode using a [testvfs] VFS. It returns a list of the 12 32-bit
# integers that make up the wal-index-header for the named file.
#
proc set_tvfs_hdr {file args} {

  # Set $nHdr to the number of bytes in the wal-index header:
  set nHdr 48
  set nInt [expr {$nHdr/4}]

  if {[llength $args]>2} {
    error {wrong # args: should be "set_tvfs_hdr fileName ?val1? ?val2?"}
  }

  set blob [tvfs shm $file]
  if {$::tcl_platform(byteOrder)=="bigEndian"} {set fmt I} {set fmt i}

  if {[llength $args]} {
    set ia [lindex $args 0]
    set ib $ia
    if {[llength $args]==2} {
      set ib [lindex $args 1]
    }
    binary scan $blob a[expr $nHdr*2]a* dummy tail
    set blob [binary format ${fmt}${nInt}${fmt}${nInt}a* $ia $ib $tail]
    tvfs shm $file $blob
  }

  binary scan $blob ${fmt}${nInt} ints
  return $ints
}

proc incr_tvfs_hdr {file idx incrval} {
  set ints [set_tvfs_hdr $file]
  set v [lindex $ints $idx]
  incr v $incrval
  lset ints $idx $v
  set_tvfs_hdr $file $ints
}
