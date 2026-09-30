/* Copyright (C) 2011 Alexey Kopytov.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef SB_PERCENTILE_H
#define SB_PERCENTILE_H

#include "timers.h"

#ifdef HAVE_PTHREAD_H
# include <pthread.h>
#endif

typedef struct {
  unsigned long long  *values;
  unsigned long long  *tmp;
  unsigned long long  total;
  unsigned int        size;
  double              range_min;
  double              range_max;
  double              range_deduct;
  double              range_mult;
  pthread_mutex_t     mutex;
} sb_percentile_t;

int sb_percentile_init(sb_percentile_t *percentile,
                       unsigned int size, double range_min, double range_max);

void sb_percentile_update(sb_percentile_t *percentile, double value);

double sb_percentile_calculate(sb_percentile_t *percentile, double percent);

void sb_percentile_reset(sb_percentile_t *percentile);

void sb_percentile_done(sb_percentile_t *percentile);

#endif
