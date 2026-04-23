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

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif
#ifdef _WIN32
#include "sb_win.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>

#include "sb_percentile.h"

int sb_percentile_init(sb_percentile_t *percentile,
                       unsigned int size, double range_min, double range_max)
{
  percentile->values = (unsigned long long *)
    calloc(size, sizeof(unsigned long long));
  percentile->tmp = (unsigned long long *)
    calloc(size, sizeof(unsigned long long));
  if (percentile->values == NULL || percentile->tmp == NULL)
  {
    //log_text(LOG_FATAL, "Cannot allocate values array, size = %u", size);
    return 1;
  }

  percentile->range_deduct = log(range_min);
  percentile->range_mult = (size - 1) / (log(range_max) -
                                         percentile->range_deduct);
  percentile->range_min = range_min;
  percentile->range_max = range_max;
  percentile->size = size;
  percentile->total = 0;

  pthread_mutex_init(&percentile->mutex, NULL);

  return 0;
}

void sb_percentile_update(sb_percentile_t *percentile, double value)
{
  unsigned int n;

  if (value < percentile->range_min)
    value= percentile->range_min;
  else if (value > percentile->range_max)
    value= percentile->range_max;

  n = floor((log(value) - percentile->range_deduct) * percentile->range_mult
            + 0.5);

  pthread_mutex_lock(&percentile->mutex);
  percentile->total++;
  percentile->values[n]++;
  pthread_mutex_unlock(&percentile->mutex);
}

double sb_percentile_calculate(sb_percentile_t *percentile, double percent)
{
  unsigned long long ncur, nmax;
  unsigned int       i;

  pthread_mutex_lock(&percentile->mutex);

  if (percentile->total == 0)
  {
    pthread_mutex_unlock(&percentile->mutex);
    return 0.0;
  }

  memcpy(percentile->tmp, percentile->values,
         percentile->size * sizeof(unsigned long long));
  nmax = floor(percentile->total * percent / 100 + 0.5);

  pthread_mutex_unlock(&percentile->mutex);

  ncur = percentile->tmp[0];
  for (i = 1; i < percentile->size; i++)
  {
    ncur += percentile->tmp[i];
    if (ncur >= nmax)
      break;
  }

  return exp((i) / percentile->range_mult + percentile->range_deduct);
}

void sb_percentile_reset(sb_percentile_t *percentile)
{
  pthread_mutex_lock(&percentile->mutex);
  percentile->total = 0;
  memset(percentile->values, 0, percentile->size * sizeof(unsigned long long));
  pthread_mutex_unlock(&percentile->mutex);
}

void sb_percentile_done(sb_percentile_t *percentile)
{
  pthread_mutex_destroy(&percentile->mutex);
  free(percentile->values);
  free(percentile->tmp);
}
