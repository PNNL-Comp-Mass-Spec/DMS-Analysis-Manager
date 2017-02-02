Imports System.Data.SqlClient
Imports System.Runtime.InteropServices
Imports System.Threading
Imports PHRPReader

Public Class clsDataPackageInfoLoader
    Inherits clsLoggerBase

    ''' <summary>
    ''' Typically Gigasax.DMS_Pipeline
    ''' </summary>
    Private ReadOnly mBrokerDbConnectionString As String

    Private ReadOnly mDataPackageID As Integer

    Private Shared mLastJobParameterFromHistoryLookup As DateTime = Date.UtcNow

    Public ReadOnly Property ConnectionString As String
        Get
            Return mBrokerDbConnectionString
        End Get
    End Property

    Public ReadOnly Property DataPackageID As Integer
        Get
            Return DataPackageID
        End Get
    End Property

    Public Sub New(brokerDbConnectionString As String, dataPackageID As Integer)
        mBrokerDbConnectionString = brokerDbConnectionString
        mDataPackageID = dataPackageID
    End Sub

    ''' <summary>
    ''' Looks up dataset information for a data package
    ''' </summary>
    ''' <param name="dctDataPackageDatasets"></param>
    ''' <returns>True if a data package is defined and it has datasets associated with it</returns>
    ''' <remarks></remarks>
    Public Function LoadDataPackageDatasetInfo(<Out> ByRef dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo)) As Boolean

        If mDataPackageID < 0 Then
            dctDataPackageDatasets = New Dictionary(Of Integer, clsDataPackageDatasetInfo)
            Return False
        Else
            Return LoadDataPackageDatasetInfo(mBrokerDbConnectionString, mDataPackageID, dctDataPackageDatasets)
        End If

    End Function

    ''' <summary>
    ''' Looks up dataset information for a data package
    ''' </summary>
    ''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
    ''' <param name="DataPackageID">Data Package ID</param>
    ''' <param name="dctDataPackageDatasets">Datasets associated with the given data package</param>
    ''' <returns>True if a data package is defined and it has datasets associated with it</returns>
    ''' <remarks></remarks>
    Public Shared Function LoadDataPackageDatasetInfo(
      connectionString As String,
      dataPackageID As Integer,
      <Out> ByRef dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo)) As Boolean

        ' Obtains the dataset information for a data package
        Const RETRY_COUNT As Short = 3

        dctDataPackageDatasets = New Dictionary(Of Integer, clsDataPackageDatasetInfo)

        Dim sqlStr = New Text.StringBuilder

        ' Note that this queries view V_DMS_Data_Package_Datasets in the DMS_Pipeline database
        ' That view references   view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database

        sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, ")
        sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
        sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType")
        sqlStr.Append(" FROM V_DMS_Data_Package_Datasets")
        sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID.ToString())
        sqlStr.Append(" ORDER BY Dataset")

        Dim resultSet As DataTable = Nothing

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageDatasetInfo", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "LoadDataPackageDatasetInfo; Excessive failures attempting to retrieve data package dataset info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            resultSet.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            Dim warningMessage = "LoadDataPackageDatasetInfo; No datasets were found for data package " & dataPackageID.ToString()
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage)
            Console.WriteLine(warningMessage)
            Return False
        End If

        For Each curRow As DataRow In resultSet.Rows
            Dim udtDatasetInfo = ParseDataPackageDatasetInfoRow(curRow)

            If Not dctDataPackageDatasets.ContainsKey(udtDatasetInfo.DatasetID) Then
                dctDataPackageDatasets.Add(udtDatasetInfo.DatasetID, udtDatasetInfo)
            End If
        Next

        resultSet.Dispose()
        Return True

    End Function

    ''' <summary>
    ''' Looks up dataset information for the data package associated with this analysis job
    ''' </summary>
    ''' <param name="dctDataPackageJobs"></param>
    ''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
    ''' <remarks></remarks>
    Protected Function LoadDataPackageJobInfo(<Out> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        If mDataPackageID < 0 Then
            dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)
            Return False
        Else
            Return LoadDataPackageJobInfo(mBrokerDbConnectionString, mDataPackageID, dctDataPackageJobs)
        End If
    End Function

    ''' <summary>
    ''' Looks up job information for a data package
    ''' </summary>
    ''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
    ''' <param name="DataPackageID">Data Package ID</param>
    ''' <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
    ''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
    ''' <remarks></remarks>
    Public Shared Function LoadDataPackageJobInfo(
      ConnectionString As String,
      DataPackageID As Integer,
      <Out> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        ' Obtains the job information for a data package
        Const RETRY_COUNT As Short = 3

        dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)

        Dim sqlStr = New Text.StringBuilder

        ' Note that this queries view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database
        ' That view references   view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database
        ' The two views have the same name, but some columns differ

        sqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ")
        sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
        sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ")
        sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,")
        sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType")
        sqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs")
        sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())
        sqlStr.Append(" ORDER BY Dataset, Tool")

        Dim resultSet As DataTable = Nothing

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve data package job info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            resultSet.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            Dim warningMessage As String

            ' If the data package exists and has datasets associated with it, then Log this as a warning but return true
            ' Otherwise, log an error and return false

            sqlStr.Clear()
            sqlStr.Append(" SELECT Count(*) AS Datasets")
            sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets")
            sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())

            ' Get a table to hold the results of the query
            success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, resultSet)
            If success AndAlso resultSet.Rows.Count > 0 Then
                For Each curRow As DataRow In resultSet.Rows
                    Dim datasetCount = clsGlobal.DbCInt(curRow(0))

                    If datasetCount > 0 Then
                        warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID &
                            ", but it does have " & datasetCount & " dataset"
                        If datasetCount > 1 Then warningMessage &= "s"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
                        Console.WriteLine(warningMessage)
                        Return True
                    End If
                Next
            End If

            warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID.ToString()
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage)
            Console.WriteLine(warningMessage)
            Return False
        End If

        For Each curRow As DataRow In resultSet.Rows
            Dim dataPkgJob = ParseDataPackageJobInfoRow(curRow)

            If Not dctDataPackageJobs.ContainsKey(dataPkgJob.Job) Then
                dctDataPackageJobs.Add(dataPkgJob.Job, dataPkgJob)
            End If
        Next

        resultSet.Dispose()

        Return True

    End Function

    Private Shared Sub LogDebugMessage(debugMessage As String)
        Console.WriteLine(debugMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, debugMessage)
    End Sub

    ''' <summary>
    ''' Retrieve the job parameters from the pipeline database for the given analysis job
    ''' The analysis job must have completed successfully, since the parameters 
    ''' are retrieved from tables T_Jobs_History, T_Job_Steps_History, and T_Job_Parameters_History
    ''' </summary>
    ''' <param name="brokerConnection">DMS_Pipline database connection (must already be open)</param>
    ''' <param name="jobNumber">Job number</param>
    ''' <param name="jobParameters">Output parameter: Dictionary of job parameters where keys are parameter names (section names are ignored)</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>This procedure is used by clsAnalysisToolRunnerPRIDEConverter</remarks>
    Private Shared Function LookupJobParametersFromHistory(
      brokerConnection As SqlConnection,
      jobNumber As Integer,
      <Out()> ByRef jobParameters As Dictionary(Of String, String),
      <Out()> ByRef errorMsg As String) As Boolean

        Const RETRY_COUNT = 3
        Const TIMEOUT_SECONDS = 30

        jobParameters = New Dictionary(Of String, String)(StringComparison.InvariantCultureIgnoreCase)
        errorMsg = String.Empty

        ' Throttle the calls to this function to avoid overloading the database for data packages with hundreds of jobs
        While Date.UtcNow.Subtract(mLastJobParameterFromHistoryLookup).TotalMilliseconds < 50
            Thread.Sleep(25)
        End While

        mLastJobParameterFromHistoryLookup = Date.UtcNow

        Try

            Dim myCmd = New SqlCommand("GetJobStepParamsAsTableUseHistory") With {
                .CommandType = CommandType.StoredProcedure,
                .Connection = brokerConnection,
                .CommandTimeout = TIMEOUT_SECONDS
            }

            myCmd.Parameters.Add(New SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue
            myCmd.Parameters.Add(New SqlParameter("@jobNumber", SqlDbType.Int)).Value = jobNumber
            myCmd.Parameters.Add(New SqlParameter("@stepNumber", SqlDbType.Int)).Value = 1

            ' Execute the SP

            Dim resultSet As DataTable = Nothing
            Dim retryCount = RETRY_COUNT
            Dim success = False

            While retryCount > 0 And Not success
                Try
                    Using Da = New SqlDataAdapter(myCmd)
                        Using Ds = New DataSet
                            Da.Fill(Ds)
                            resultSet = Ds.Tables(0)
                        End Using
                    End Using

                    success = True
                Catch ex As Exception
                    retryCount -= 1S
                    Dim msg = "Exception running stored procedure " & myCmd.CommandText & ": " + ex.Message + "; RetryCount = " + retryCount.ToString

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
                    Console.WriteLine(msg)

                    Thread.Sleep(5000)              ' Delay for 5 second before trying again
                End Try
            End While

            If Not success Then
                errorMsg = "Unable to retrieve job parameters from history for job " & jobNumber
                Return False
            End If

            ' Verify at least one row returned
            If resultSet.Rows.Count < 1 Then
                ' No data was returned
                ' Log an error

                errorMsg = "Historical parameters were not found for job " & jobNumber
                Return False
            End If

            For Each curRow As DataRow In resultSet.Rows
                ' Dim section = clsGlobal.DbCStr(curRow(0))
                Dim parameter = clsGlobal.DbCStr(curRow(1))
                Dim value = clsGlobal.DbCStr(curRow(2))

                If jobParameters.ContainsKey(parameter) Then
                    Dim msg = "Job " & jobNumber & " has multiple values for parameter " & parameter & "; only using the first occurrence"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                    Console.WriteLine(msg)
                Else
                    jobParameters.Add(parameter, value)
                End If
            Next

            resultSet.Dispose()

            Return True

        Catch ex As Exception
            errorMsg = "Exception retrieving parameters from history for job " & jobNumber
            Dim detailedMsg = "Exception retrieving parameters from history for job " & jobNumber & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMsg)
            Console.WriteLine(detailedMsg)
            Return False
        End Try

    End Function

    Private Shared Function ParseDataPackageDatasetInfoRow(curRow As DataRow) As clsDataPackageDatasetInfo

        Dim datasetName = clsGlobal.DbCStr(curRow("Dataset"))
        Dim datasetId = clsGlobal.DbCInt(curRow("DatasetID"))

        Dim datasetInfo = New clsDataPackageDatasetInfo(datasetName, datasetId)

        datasetInfo.Instrument = clsGlobal.DbCStr(curRow("Instrument"))
        datasetInfo.InstrumentGroup = clsGlobal.DbCStr(curRow("InstrumentGroup"))
        datasetInfo.Experiment = clsGlobal.DbCStr(curRow("Experiment"))
        datasetInfo.Experiment_Reason = clsGlobal.DbCStr(curRow("Experiment_Reason"))
        datasetInfo.Experiment_Comment = clsGlobal.DbCStr(curRow("Experiment_Comment"))
        datasetInfo.Experiment_Organism = clsGlobal.DbCStr(curRow("Organism"))
        datasetInfo.Experiment_NEWT_ID = clsGlobal.DbCInt(curRow("Experiment_NEWT_ID"))
        datasetInfo.Experiment_NEWT_Name = clsGlobal.DbCStr(curRow("Experiment_NEWT_Name"))
        datasetInfo.ServerStoragePath = clsGlobal.DbCStr(curRow("Dataset_Folder_Path"))
        datasetInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow("Archive_Folder_Path"))
        datasetInfo.RawDataType = clsGlobal.DbCStr(curRow("RawDataType"))

        Return datasetInfo

    End Function

    Private Shared Function ParseDataPackageJobInfoRow(curRow As DataRow) As clsDataPackageJobInfo

        Dim dataPkgJob = clsGlobal.DbCInt(curRow("Job"))
        Dim dataPkgDataset = clsGlobal.DbCStr(curRow("Dataset"))

        Dim jobInfo = New clsDataPackageJobInfo(dataPkgJob, dataPkgDataset)

        jobInfo.DatasetID = clsGlobal.DbCInt(curRow("DatasetID"))
        jobInfo.Instrument = clsGlobal.DbCStr(curRow("Instrument"))
        jobInfo.InstrumentGroup = clsGlobal.DbCStr(curRow("InstrumentGroup"))
        jobInfo.Experiment = clsGlobal.DbCStr(curRow("Experiment"))
        jobInfo.Experiment_Reason = clsGlobal.DbCStr(curRow("Experiment_Reason"))
        jobInfo.Experiment_Comment = clsGlobal.DbCStr(curRow("Experiment_Comment"))
        jobInfo.Experiment_Organism = clsGlobal.DbCStr(curRow("Organism"))
        jobInfo.Experiment_NEWT_ID = clsGlobal.DbCInt(curRow("Experiment_NEWT_ID"))
        jobInfo.Experiment_NEWT_Name = clsGlobal.DbCStr(curRow("Experiment_NEWT_Name"))
        jobInfo.Tool = clsGlobal.DbCStr(curRow("Tool"))
        jobInfo.ResultType = clsGlobal.DbCStr(curRow("ResultType"))
        jobInfo.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(jobInfo.ResultType)
        jobInfo.SettingsFileName = clsGlobal.DbCStr(curRow("SettingsFileName"))
        jobInfo.ParameterFileName = clsGlobal.DbCStr(curRow("ParameterFileName"))
        jobInfo.OrganismDBName = clsGlobal.DbCStr(curRow("OrganismDBName"))
        jobInfo.ProteinCollectionList = clsGlobal.DbCStr(curRow("ProteinCollectionList"))
        jobInfo.ProteinOptions = clsGlobal.DbCStr(curRow("ProteinOptions"))

        ' This will be updated later for SplitFasta jobs (using function LookupJobParametersFromHistory)
        jobInfo.NumberOfClonedSteps = 0

        If String.IsNullOrWhiteSpace(jobInfo.ProteinCollectionList) OrElse jobInfo.ProteinCollectionList = "na" Then
            jobInfo.LegacyFastaFileName = String.Copy(jobInfo.OrganismDBName)
        Else
            jobInfo.LegacyFastaFileName = "na"
        End If

        jobInfo.ServerStoragePath = clsGlobal.DbCStr(curRow("ServerStoragePath"))
        jobInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow("ArchiveStoragePath"))
        jobInfo.ResultsFolderName = clsGlobal.DbCStr(curRow("ResultsFolder"))
        jobInfo.DatasetFolderName = clsGlobal.DbCStr(curRow("DatasetFolder"))
        jobInfo.SharedResultsFolder = clsGlobal.DbCStr(curRow("SharedResultsFolder"))
        jobInfo.RawDataType = clsGlobal.DbCStr(curRow("RawDataType"))

        Return jobInfo

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDataPackagePeptideHitJobInfo() As List(Of clsDataPackageJobInfo)

        Dim lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
        Return RetrieveDataPackagePeptideHitJobInfo(lstAdditionalJobs)

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="lstAdditionalJobs">Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Public Function RetrieveDataPackagePeptideHitJobInfo(
      <Out()> ByRef lstAdditionalJobs As List(Of clsDataPackageJobInfo)) As List(Of clsDataPackageJobInfo)

        ' Gigasax.DMS_Pipeline
        Dim connectionString As String = mBrokerDbConnectionString

        If mDataPackageID < 0 Then
            LogError("DataPackageID is not defined for this analysis job")
            lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
            Return New List(Of clsDataPackageJobInfo)
        Else
            Dim errorMsg As String = String.Empty
            Dim lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(connectionString, mDataPackageID, lstAdditionalJobs, errorMsg)

            If Not String.IsNullOrWhiteSpace(errorMsg) Then
                LogError(errorMsg)
            End If

            Return lstDataPackagePeptideHitJobs
        End If

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="connectionString">Connection string to the DMS_Pipeline database</param>
    ''' <param name="dataPackageID">Data package ID</param>
    ''' <param name="errorMsg">Output: error message</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Public Shared Function RetrieveDataPackagePeptideHitJobInfo(
       connectionString As String,
       dataPackageID As Integer,
       <Out()> errorMsg As String) As List(Of clsDataPackageJobInfo)

        Dim lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
        Return RetrieveDataPackagePeptideHitJobInfo(connectionString, dataPackageID, lstAdditionalJobs, errorMsg)

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="connectionString">Connection string to the DMS_Pipeline database</param>
    ''' <param name="dataPackageID">Data package ID</param>
    ''' <param name="lstAdditionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
    ''' <param name="errorMsg">Output: error message</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Public Shared Function RetrieveDataPackagePeptideHitJobInfo(
      connectionString As String,
      dataPackageID As Integer,
      <Out()> ByRef lstAdditionalJobs As List(Of clsDataPackageJobInfo),
      <Out()> ByRef errorMsg As String) As List(Of clsDataPackageJobInfo)

        Dim lstDataPackagePeptideHitJobs As List(Of clsDataPackageJobInfo)
        Dim dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)

        ' This list tracks the info for the Peptide Hit jobs (e.g. MSGF+ or Sequest) associated with this aggregation job's data package
        lstDataPackagePeptideHitJobs = New List(Of clsDataPackageJobInfo)
        errorMsg = String.Empty

        ' This list tracks the info for the non Peptide Hit jobs (e.g. DeconTools or MASIC) associated with this aggregation job's data package
        lstAdditionalJobs = New List(Of clsDataPackageJobInfo)

        ' This dictionary will track the jobs associated with this aggregation job's data package
        ' Key is job number, value is an instance of clsDataPackageJobInfo
        dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)

        Try
            If Not LoadDataPackageJobInfo(connectionString, dataPackageID, dctDataPackageJobs) Then
                errorMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
                Return lstDataPackagePeptideHitJobs
            End If
        Catch ex As Exception
            errorMsg = "Exception calling LoadDataPackageJobInfo: " & ex.Message
            Return lstDataPackagePeptideHitJobs
        End Try

        Try
            For Each kvItem As KeyValuePair(Of Integer, clsDataPackageJobInfo) In dctDataPackageJobs

                If kvItem.Value.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    lstAdditionalJobs.Add(kvItem.Value)
                Else
                    ' Cache this job info in lstDataPackagePeptideHitJobs
                    lstDataPackagePeptideHitJobs.Add(kvItem.Value)
                End If

            Next

        Catch ex As Exception
            errorMsg = "Exception determining data package jobs for this aggregation job (RetrieveDataPackagePeptideHitJobInfo): " & ex.Message
        End Try

        Try
            ' Look for any SplitFasta jobs
            ' If present, we need to determine the value for job parameter NumberOfClonedSteps

            Dim splitFastaJobs = (From dataPkgJob In lstDataPackagePeptideHitJobs
                                  Where dataPkgJob.Tool.ToLower().Contains("splitfasta")
                                  Select dataPkgJob).ToList()

            If splitFastaJobs.Count > 0 Then

                Dim lastStatusTime = Date.UtcNow
                Dim statusIntervalSeconds = 4
                Dim jobsProcessed = 0

                Using brokerConnection = New SqlConnection(connectionString)
                    brokerConnection.Open()

                    For Each dataPkgJob In splitFastaJobs

                        Dim dataPkgJobParameters As Dictionary(Of String, String) = Nothing

                        Dim success = LookupJobParametersFromHistory(brokerConnection, dataPkgJob.Job, dataPkgJobParameters, errorMsg)

                        If Not success Then
                            Return New List(Of clsDataPackageJobInfo)
                        End If

                        Dim numberOfClonedSteps As String = Nothing
                        If dataPkgJobParameters.TryGetValue("NumberOfClonedSteps", numberOfClonedSteps) Then
                            Dim clonedStepCount = CInt(numberOfClonedSteps)
                            dataPkgJob.NumberOfClonedSteps = clonedStepCount
                        End If

                        jobsProcessed += 1

                        If Date.UtcNow.Subtract(lastStatusTime).TotalSeconds >= statusIntervalSeconds Then
                            Dim pctComplete = jobsProcessed / splitFastaJobs.Count * 100
                            LogDebugMessage("Retrieving job parameters from history for SplitFasta jobs; " & pctComplete.ToString("0") & "% complete")

                            lastStatusTime = Date.UtcNow

                            ' Double the status interval, allowing for a maximum of 30 seconds
                            statusIntervalSeconds = Math.Min(30, statusIntervalSeconds * 2)

                        End If
                    Next
                End Using

            End If


        Catch ex As Exception
            errorMsg = "Exception calling LookupJobParametersFromHistory (RetrieveDataPackagePeptideHitJobInfo): " & ex.Message
            Return New List(Of clsDataPackageJobInfo)
        End Try

        Return lstDataPackagePeptideHitJobs

    End Function


End Class
