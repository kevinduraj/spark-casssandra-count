#!/usr/bin/bash
#---------------------------------------------------------------------# 
number_of_nodes=3
tables="cloud1.vdomain cloud1.ldomain cloud1.visit cloud1.link"
#---------------------------------------------------------------------# 
nicenumber()
{
  integer=$(echo $1 | cut -d. -f1)      # left of the decimal
  decimal=$(echo $1 | cut -d. -f2)      # right of the decimal

  if [ $decimal != $1 ]; then
    result="${DD:="."}$decimal"
  fi

  thousands=$integer
  result=''

  while [ $thousands -gt 999 ]; do
    remainder=$(($thousands % 1000))    # three least significant digits

    while [ ${#remainder} -lt 3 ] ; do  # force leading zeros as needed
      remainder="0$remainder"
    done

    thousands=$(($thousands / 1000))    # to left of remainder, if any
    result="${TD:=","}${remainder}${result}"    # builds right to left
  done

  nicenum="${thousands}${result}"
  printf "  %-15s %16s\n" $2 "$nicenum"
}
#---------------------------------------------------------------------------# 
estimate() {

  for table in $tables 
  do
    number=$(nodetool cfstats $table | grep "Number of keys" | awk '{print $5}' )
    total="$(($number * $number_of_nodes))"
    nicenumber $total $table;
   
    # substract 3%
    # percent="$(($total/100))"
    # total2="$(($total - ( $percent * 3)))"
    # nicenumber $total2 $table;
  done
}
#---------------------------------------------------------------------------# 

estimate
