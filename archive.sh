
YEAR_MONTH=$1

FIRST_ZIP=$(ls -1 ${YEAR_MONTH}-*.zip 2>/dev/null | head -n 1)FIRST_ZIP=$(ls -1 *.zip | head -n 1)

#FIRST_CSV=$(unzip -l "$FIRST_ZIP" | grep '\.csv' | head -n 1 | awk '{print $NF}')

#unzip -p "$FIRST_ZIP" "$FIRST_CSV" | head -n 1

