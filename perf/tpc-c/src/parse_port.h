inline void parse_host(char* host, const char* from)
{
  const char* port= rindex(from,':');
  size_t length;
  if(NULL == port)
    length= strlen(from);
  else
    length= (port - from);
  memcpy(host,from, length);
  host[length] = '\0';
}
inline int parse_port(const char* from)
{
  const char* port= rindex(from,':');
  if(NULL == port)
    return 3306;
  else
  {
    const char* end= NULL;
    int result= strtol(port+1,&end,10);
    if( (0 == *end) && (0 <= result) && (result <= 0xFFFF) )
      return result;
    else
    {
      printf(stderr,"Incorrect port value: %s\n",end);
      exit(-1);
    }
  }
}
