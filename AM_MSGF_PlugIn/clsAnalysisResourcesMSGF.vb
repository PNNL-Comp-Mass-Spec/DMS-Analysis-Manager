'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGF
	Inherits clsAnalysisResources

	'*********************************************************************************************************
    'Manages retrieval of all files needed by MSGF
	'*********************************************************************************************************

#Region "Constants"
#End Region

#Region "Module variables"
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
    ''' Gets all files needed by MSGF
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        ' Make sure the machine has enough free memory to run MSGF
        eResult = ValidateFreeMemorySize()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then            
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Get analysis results files
        eResult = GetInputFiles(m_jobParams.GetParam("ResultType"))
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function GetFreeMemoryMB() As Single

        Static mFreeMemoryPerformanceCounter As System.Diagnostics.PerformanceCounter

        Dim sngFreeMemory As Single

        Try
            If mFreeMemoryPerformanceCounter Is Nothing Then
                mFreeMemoryPerformanceCounter = New System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
                mFreeMemoryPerformanceCounter.ReadOnly = True
            End If

            sngFreeMemory = mFreeMemoryPerformanceCounter.NextValue()

        Catch ex As Exception
            ' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
            If System.DateTime.Now().Hour = 0 And System.DateTime.Now().Minute <= 30 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " & ex.Message)
            End If
        End Try

        Return sngFreeMemory

    End Function

	''' <summary>
    ''' Retrieves input files needed for MSGF
	''' </summary>
	''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal ResultType As String) As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResultType As clsMSGFRunner.ePeptideHitResultType

        Dim DatasetName As String
        Dim RawDataType As String

        Dim FileToGet As String
        Dim strMzXMLFilePath As String = String.Empty

        Dim blnSuccess As Boolean = False

        ' Cache the dataset name
        DatasetName = m_jobParams.GetParam("DatasetNum")

        ' Make sure the ResultType is valid
        eResultType = clsMSGFRunner.GetPeptideHitResultType(ResultType)

        If eResultType = clsMSGFRunner.ePeptideHitResultType.Sequest OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.XTandem OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.Inspect OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.MSGFDB Then
            blnSuccess = True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid tool result type (not supported by MSGF): " & ResultType)
            blnSuccess = False
        End If
        
        If Not blnSuccess Then
            Return (IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES)
        End If

        ' Make sure the dataset type is valid
        RawDataType = m_jobParams.GetParam("RawDataType")

        If RawDataType.ToLower <> RAW_DATA_TYPE_DOT_RAW_FILES Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Dataset type " & RawDataType & " is not supported; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        ' Get the Sequest, X!Tandem, Inspect, or MSGF-DB parameter file
        FileToGet = m_jobParams.GetParam("ParmFileName")
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        clsGlobal.FilesToDelete.Add(FileToGet)


        ' Get the ModSummary.txt file        
        FileToGet = clsMSGFRunner.GetModSummaryFileName(eResultType, DatasetName)
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        clsGlobal.FilesToDelete.Add(FileToGet)

        ' Get the Sequest, X!Tandem, Inspect, or MSGF-DB PHRP _syn.txt file
        FileToGet = clsMSGFRunner.GetPHRPSynopsisFileName(eResultType, DatasetName)
        If Not String.IsNullOrEmpty(FileToGet) Then
            If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            clsGlobal.FilesToDelete.Add(FileToGet)
        End If

        ' Get the Sequest, X!Tandem, or Inspect PHRP _fht.txt file
        FileToGet = clsMSGFRunner.GetPHRPFirstHitsFileName(eResultType, DatasetName)
        If Not String.IsNullOrEmpty(FileToGet) Then
            If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            clsGlobal.FilesToDelete.Add(FileToGet)
        End If

        If eResultType = clsMSGFRunner.ePeptideHitResultType.XTandem Then
            ' Grab a few more files for X!Tandem files so that we can extract the protein names and include these in the MSGF files

            FileToGet = DatasetName & clsMSGFRunner.XT_RESULT_TO_SEQ_MAP_SUFFIX
            If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            clsGlobal.FilesToDelete.Add(FileToGet)

            FileToGet = DatasetName & clsMSGFRunner.XT_SEQ_TO_PROTEIN_MAP_SUFFIX
            If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
            End If
            clsGlobal.FilesToDelete.Add(FileToGet)
        End If

        ' See if a .mzXML file already exists for this dataset
        blnSuccess = RetrieveMZXmlFile(m_WorkingDir, False, strMzXMLFilePath)

        ' Make sure we don't move the .mzXML file into the results folder
        clsGlobal.m_FilesToDeleteExt.Add(".mzXML")

        If blnSuccess Then
            ' .mzXML file found and copied locally; no need to retrieve the .Raw file
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Existing .mzXML file found: " & strMzXMLFilePath)
            End If
        Else
            ' .mzXML file not found
            ' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
            If RetrieveSpectra(RawDataType, m_WorkingDir) Then
                clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_RAW_EXTENSION)            ' Raw file
                clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_MZXML_EXTENSION)          ' mzXML file
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Lookups the amount of memory that will be reserved for Java
    ''' If this value is >= the free memory, then returns CLOSEOUT_FAILED
    ''' Otherwise, returns CLOSEOUT_SUCCESS
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ValidateFreeMemorySize() As IJobParams.CloseOutType

        Dim intJavaMemorySizeMB As Integer
        Dim sngFreeMemoryMB As Single
        Dim strMessage As String

        intJavaMemorySizeMB = clsGlobal.GetJobParameter(m_jobParams, "MSGFJavaMemorySize", 2000)
        If intJavaMemorySizeMB < 512 Then intJavaMemorySizeMB = 512

        sngFreeMemoryMB = GetFreeMemoryMB()

        If intJavaMemorySizeMB >= sngFreeMemoryMB Then
            strMessage = "Not enough free memory to run MSGF"

            strMessage &= "; need " & intJavaMemorySizeMB & " MB but system has " & sngFreeMemoryMB.ToString("0") & " MB available"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            If m_DebugLevel >= 1 Then
                strMessage = "MSGF will use " & intJavaMemorySizeMB & " MB; system has " & sngFreeMemoryMB.ToString("0") & " MB available"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If
    End Function

#End Region

End Class
