/*
 * sequence.c
 * manage sequence shared by threads
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

/* weight */
static int no;
static int py;
static int os;
static int dl;
static int sl;
static int total;

static pthread_mutex_t mutex;
static int *seq;
static int next_num;

static void shuffle()
{
  int i,j,rnd,tmp;

  for( i=0, j=0; i < no ; i++, j++ ){
    seq[j]=0;
  }
  for( i=0; i < py ; i++, j++){
    seq[j]=1;
  }
  for( i=0; i < os ; i++, j++){
    seq[j]=2;
  }
  for( i=0; i < dl ; i++, j++){
    seq[j]=3;
  }
  for( i=0; i < sl ; i++, j++){
    seq[j]=4;
  }
  for( i=0, j = total - 1; j>0; i++, j--){
    rnd = rand()%(j+1);
    tmp = seq[rnd+i];
    seq[rnd+i] = seq[i];
    seq[i] = tmp;
  }
}

void seq_init( int n, int p, int o, int d, int s )
{
  pthread_mutex_init( &mutex, NULL );
  no = n;
  py = p;
  os = o;
  dl = d;
  sl = s;
  total = n + p + o + d + s;
  seq = malloc( sizeof(int) * total );
  shuffle();
  next_num = 0;
}


int seq_get()
{
  int retval;

  pthread_mutex_lock( &mutex );

  if(next_num >= total){
    shuffle();
    next_num = 0;
  }

  retval = seq[next_num];
  ++next_num;

  pthread_mutex_unlock( &mutex );

  return(retval);
}
