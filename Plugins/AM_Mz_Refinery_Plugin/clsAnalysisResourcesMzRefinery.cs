Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMzRefinery
    Inherits clsAnalysisResources

#Region "Methods"

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    ''' <summary>
    ''' Retrieves files necessary for running MzRefinery
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim currentTask = "Initializing"

        Try

            currentTask = "Retrieve shared resources"

            ' Retrieve shared resources, including the JobParameters file from the previous job step
            GetSharedResources()

            Dim mzRefParamFile = m_jobParams.GetJobParameter("MzRefParamFile", String.Empty)
            If String.IsNullOrEmpty(mzRefParamFile) Then
                LogError("MzRefParamFile parameter is empty")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            currentTask = "Get Input file"

            Dim eResult As IJobParams.CloseOutType
            eResult = GetMsXmlFile()

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If

            ' Retrieve the Fasta file
            Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

            currentTask = "RetrieveOrgDB to " & localOrgDbFolder

            If Not RetrieveOrgDB(localOrgDbFolder) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Retrieve the Mz Refinery parameter file
            currentTask = "Retrieve the Mz Refinery parameter file " & mzRefParamFile

            Const paramFileStoragePathKeyName As String = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "Mz_Refinery"

            Dim mzRefineryParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName)
            If String.IsNullOrWhiteSpace(mzRefineryParmFileStoragePath) Then
                mzRefineryParmFileStoragePath = "\\gigasax\dms_parameter_Files\MzRefinery"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & paramFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & mzRefineryParmFileStoragePath)
            End If

            If Not RetrieveFile(mzRefParamFile, mzRefineryParmFileStoragePath) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Look for existing MSGF+ results in the transfer folder
            currentTask = "Find existing MSGF+ results"

            If Not FindExistingMSGFPlusResults(mzRefParamFile) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Exception in GetResources: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Check for existing MSGF+ results in the transfer directory
    ''' </summary>
    ''' <returns>True if no errors, false if a problem</returns>
    ''' <remarks>Will retrun True even if existing results are not found</remarks>
    Private Function FindExistingMSGFPlusResults(mzRefParamFileName As String) As Boolean

        Dim resultsFolderName = m_jobParams.GetParam("OutputFolderName")
        Dim transferFolderPath = m_jobParams.GetParam("transferFolderPath")

        If String.IsNullOrWhiteSpace(resultsFolderName) Then
            m_message = "Results folder not defined (job parameter OutputFolderName)"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End If

        If String.IsNullOrWhiteSpace(transferFolderPath) Then
            m_message = "Transfer folder not defined (job parameter transferFolderPath)"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End If

        Dim diTransferFolder = New DirectoryInfo(Path.Combine(transferFolderPath, m_DatasetName, resultsFolderName))
        If Not diTransferFolder.Exists Then
            ' This is not an error -- it just means there are no existing MSGF+ results to use
            Return True
        End If

        ' Look for the required files in the transfer folder
        Dim resultsFileName = m_DatasetName & clsAnalysisToolRunnerMzRefinery.MSGFPLUS_MZID_SUFFIX & ".gz"
        Dim fiMSGFPlusResults = New FileInfo(Path.Combine(diTransferFolder.FullName, resultsFileName))

        If Not fiMSGFPlusResults.Exists Then
            ' This is not an error -- it just means there are no existing MSGF+ results to use
            Return True
        End If

        Dim fiMSGFPlusConsoleOutput = New FileInfo(Path.Combine(diTransferFolder.FullName, "MSGFPlus_ConsoleOutput.txt"))
        If Not fiMSGFPlusResults.Exists Then
            ' This is unusual; typically if the mzid.gz file exists there should be a ConsoleOutput file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Found " & fiMSGFPlusResults.FullName & " but did not find " & fiMSGFPlusConsoleOutput.Name & "; will re-run MSGF+")
            Return True
        End If

        Dim fiMzRefParamFile = New FileInfo(Path.Combine(diTransferFolder.FullName, mzRefParamFileName))
        If Not fiMzRefParamFile.Exists Then
            ' This is unusual; typically if the mzid.gz file exists there should be a MzRefinery parameter file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Found " & fiMSGFPlusResults.FullName & " but did not find " & fiMzRefParamFile.Name & "; will re-run MSGF+")
            Return True
        End If

        ' Compare the remote parameter file and the local one to make sure they match
        If Not clsGlobal.TextFilesMatch(fiMzRefParamFile.FullName, Path.Combine(m_WorkingDir, mzRefParamFileName), True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MzRefinery parameter file in transfer folder does not match the official MzRefinery paramter file; will re-run MSGF+")
            Return True
        End If

        ' Existing results found
        ' Copy the MSGF+ results locally
        Dim localFilePath = Path.Combine(m_WorkingDir, fiMSGFPlusResults.Name)
        fiMSGFPlusResults.CopyTo(localFilePath, True)

        m_IonicZipTools.GUnzipFile(localFilePath)

        localFilePath = Path.Combine(m_WorkingDir, fiMSGFPlusConsoleOutput.Name)
        fiMSGFPlusConsoleOutput.CopyTo(localFilePath, True)

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Found existing MSGF+ results to use for MzRefinery")

        Return True

    End Function

#End Region

End Class
