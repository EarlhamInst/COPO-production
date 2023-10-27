# NB: Make bash script executable before running it
# Replace the data in the 'input_data.txt' file with actual values before running the script
# Run script with the command: $ ./extractSraAccessionFromENA.sh < input_data.txt > output_data.txt
while read -r line
do
IFS=' ' read -ra ADDR <<< "$line"
biosampleAccession=${ADDR[0]}
# NB: Decimal latitude and longitude values must be rounded to 8 decimal places
# Uncomment the following lines to round the latitude and longitude values
# lat=`echo ${ADDR[1]} | xargs printf '%.*f' 8`
# long=`echo ${ADDR[2]} | xargs printf '%.*f' 8`
lat=${ADDR[1]}
long=${ADDR[2]}

output=$(curl -s -X POST -H 'Content-Type: application/x-www-form-urlencoded' -d "result=sample&query=accession=${biosampleAccession}&fields=secondary_sample_accession&format=tsv" 'https://www.ebi.ac.uk/ena/portal/api/search')
  sraAccession=`echo $output | sed 's/.*\ERS\(.*\)$/\1/'`
  echo "{"
  echo '"sample_accession": "'ERS$sraAccession'",'
  echo '"geographic location (latitude)" : "'$lat'",'
  echo '"geographic location (longitude)" : "'$long'"'
  echo "}," 
done