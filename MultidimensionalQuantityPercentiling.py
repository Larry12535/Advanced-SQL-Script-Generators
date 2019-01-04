def generateSegQuery(table, userColumn, categoryColumns, quantityColumn, numPartitions):
    table = f'"{table}"'
    userColumn = f'"{userColumn}"'
    quantityColumn = f'"{quantityColumn}"'
    categoryColumns = [f'"{col}"' for col in categoryColumns]

    QUERY = 'WITH '
    #get running totals
    QUERY += f'''runningtotals AS (
        {getRunningTotalQuery(table, userColumn, categoryColumns, quantityColumn)}
    ),
    '''
    #get category sums
    QUERY += f'''
        sumsbypartition AS (
            {getSumByCategory(table, categoryColumns, quantityColumn)}
        ),
    '''
    #get partitions
    QUERY += f'partitions AS ({getPartitions(numPartitions)}),'
    #get distinct column values
    for col in categoryColumns:
        QUERY += f'''{col} AS (
            {getDistinctValuesByColumn(table, col)}
        ),'''
    #get cross Join Partitions With Partitioning Columns
    QUERY += f'''
        crossjoins AS (
            {crossJoin('partitions', [f'{column}' for column in categoryColumns])}
        ),
    '''
    #get partition ranges
    QUERY += f'''
        partitionquantityranges AS (
            {partitionQuantityRanges('crossjoins', 'sumsbypartition', categoryColumns, numPartitions)}
        )
    '''
    QUERY += decileRunningTotals('runningtotals', 'partitionquantityranges', categoryColumns)
    return QUERY

def decileRunningTotals(runningtotals, partitionquantityranges, categoryColumns):
    joinConditions = ''
    for column in categoryColumns:
        joinConditions += f'{runningtotals}.{column} = {partitionquantityranges}.{column} AND '
    joinConditions = joinConditions + f'{runningtotals}.runningtotal >= {partitionquantityranges}.lowerbound AND {runningtotals}.runningtotal < {partitionquantityranges}.upperbound'

    QUERY = f'''
        SELECT
            {runningtotals}.*,
            {partitionquantityranges}.partition
        FROM {runningtotals}
        LEFT JOIN {partitionquantityranges} ON {joinConditions}
    '''
    return QUERY

def partitionQuantityRanges(crossjoinTable, sumsTable, categoryColumns, numPartitions):
    leftJoinConditions = ''
    for column in categoryColumns:
        leftJoinConditions += f'{sumsTable}.{column} = {crossjoinTable}.{column} AND '
    leftJoinConditions = leftJoinConditions[:-5]

    QUERY = f'''
        SELECT
            {crossjoinTable}.*,
            ({crossjoinTable}.partition - 1) * {sumsTable}.sum/{numPartitions} AS lowerBound,
            CASE WHEN {crossjoinTable}.partition = {numPartitions} THEN {crossjoinTable}.partition * {sumsTable}.sum/{numPartitions} + 1
                 ELSE {crossjoinTable}.partition * "{sumsTable}".sum/{numPartitions}
            END AS upperBound
        FROM {crossjoinTable}
        {f'LEFT JOIN {sumsTable} ON {leftJoinConditions}' if leftJoinConditions 
        else f'CROSS JOIN {sumsTable}'}
    '''
    return QUERY

def crossJoin(mainTable, joinTables):
    QUERY = f'''
        SELECT
            *
        FROM {mainTable}
    '''
    for table in joinTables:
        QUERY += f'''CROSS JOIN {table}
        '''
    return QUERY

def getPartitions(numPartitions):
    QUERY = f'''
        SELECT
            generate_series AS partition
        FROM generate_series(1, {numPartitions})
    '''
    return QUERY

def getRunningTotalQuery(table, userColumn, categoryColumns, quantityColumn):
    categoryColumnselects = getCategoryColumnselects(categoryColumns)
    partitionBy = f'PARTITION BY {categoryColumnselects[:-1]}' if len(categoryColumns) > 0 else ''
    QUERY = f'''
        SELECT
            {userColumn},
            {getCategoryColumnselects(categoryColumns)}
            {quantityColumn},
            SUM({quantityColumn}) OVER ({partitionBy} ORDER BY {quantityColumn} NULLS LAST ROWS UNBOUNDED PRECEDING) AS runningTotal
        FROM {table}
    '''
    return QUERY

def getDistinctValuesByColumn(table, column):
    return f'SELECT DISTINCT({column}) FROM {table}'

def getSumByCategory(table, categoryColumns, quantityColumn):
    categoryColumnsSelect = getCategoryColumnselects(categoryColumns)[:-1]
    QUERY = f'''
        SELECT
            {categoryColumnsSelect}{',' if categoryColumnsSelect else ''}
            SUM({quantityColumn}) AS sum
        FROM {table}
        {f'GROUP BY {categoryColumnsSelect}' if len(categoryColumns) > 0 else ''}
    '''
    return QUERY[:-1]

def getCategoryColumnselects(categoryColumns):
    categoryColumnselects = ''
    for col in categoryColumns:
        categoryColumnselects += f'{col},'
    return categoryColumnselects

segquery = generateSegQuery('employees', 'name', ['floor'], 'salary', 10)
print(segquery)
