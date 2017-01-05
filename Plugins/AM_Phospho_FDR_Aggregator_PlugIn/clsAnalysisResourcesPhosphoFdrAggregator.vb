Option Strict On

Imports AnalysisManagerBase
Imports System.Linq
Imports System.IO
Imports System.Text.RegularExpressions


Public Class clsAnalysisResourcesPhosphoFdrAggregator
    Inherits clsAnalysisResources

    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Lookup the file processing options, for example:
        ' sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
        ' MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,masic_finnigan:_ScanStatsEx.txt

        Dim fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(","c).ToList()

        For Each fileSpec As String In fileSpecList.ToList()
            Dim fileSpecTerms = fileSpec.Split(":"c).ToList()
            If fileSpecTerms.Count <= 2 OrElse Not fileSpecTerms(2).ToLower().Trim() = "copy" Then
                m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms(1))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        Dim paramFilesCopied = 0

        If Not RetrieveAScoreParamfile("AScoreCIDParamFile", paramFilesCopied) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveAScoreParamfile("AScoreETDParamFile", paramFilesCopied) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveAScoreParamfile("AScoreHCDParamFile", paramFilesCopied) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If paramFilesCopied = 0 Then
            m_message = "One more more of these job parameters must define a valid AScore parameter file name: AScoreCIDParamFile, AScoreETDParamFile, or AScoreHCDParamFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files")

        Dim dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo) = Nothing

        ' Retrieve the files for the jobs in the data package associated with this job
        If Not RetrieveAggregateFiles(fileSpecList, clsAnalysisResources.DataPackageFileRetrievalModeConstants.Ascore, dctDataPackageJobs) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Cache the data package info
        If Not CacheDataPackageInfo(dctDataPackageJobs) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function CacheDataPackageInfo(dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        Try

            Dim diWorkingFolder = New DirectoryInfo(m_WorkingDir)

            Dim jobToDatasetMap = New Dictionary(Of String, String)
            Dim jobToSettingsFileMap = New Dictionary(Of String, String)
            Dim jobToolMap = New Dictionary(Of String, String)

            ' Find the Job* folders
            For Each subFolder In diWorkingFolder.GetDirectories("Job*")

                Dim jobNumber = Integer.Parse(subFolder.Name.Substring(3))
                Dim udtJobInfo = dctDataPackageJobs(jobNumber)

                jobToDatasetMap.Add(jobNumber.ToString(), udtJobInfo.Dataset)
                jobToSettingsFileMap.Add(jobNumber.ToString(), udtJobInfo.SettingsFileName)
                jobToolMap.Add(jobNumber.ToString(), udtJobInfo.Tool)
            Next

            ' Store the packed job parameters
            StorePackedJobParameterDictionary(jobToDatasetMap, JOB_PARAM_DICTIONARY_JOB_DATASET_MAP)
            StorePackedJobParameterDictionary(jobToSettingsFileMap, JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP)
            StorePackedJobParameterDictionary(jobToolMap, JOB_PARAM_DICTIONARY_JOB_TOOL_MAP)

        Catch ex As Exception
            m_message = "Error in CacheDataPackageInfo"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CacheDataPackageInfo: " & ex.Message)
            Return False
        End Try

        Return True
    End Function

    ''' <summary>
    ''' Retrieves the AScore parameter file stored in the given parameter name
    ''' </summary>
    ''' <param name="parameterName">AScoreCIDParamFile or AScoreETDParamFile or AScoreHCDParamFile</param>
    ''' <param name="paramFilesCopied">Incremented if the parameter file is found and copied</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RetrieveAScoreParamfile(parameterName As String, ByRef paramFilesCopied As Integer) As Boolean

        Dim paramFileName = m_jobParams.GetJobParameter(parameterName, String.Empty)
        If String.IsNullOrWhiteSpace(paramFileName) Then
            Return True
        End If

        If paramFileName.ToLower().StartsWith("xxx_ascore_") OrElse paramFileName.ToLower().StartsWith("xxx_undefined") Then
            ' Dummy parameter file; ignore it
            ' Update the job parameter to be an empty string so that this parameter is ignored in BuildInputFile
            m_jobParams.SetParam(parameterName, String.Empty)
            Return True
        End If

        Dim success = RetrieveFile(paramFileName, m_jobParams.GetParam("transferFolderPath"), 2, clsLogTools.LogLevels.DEBUG)

        If Not success Then
            ' File not found in the transfer folder
            ' Look in the AScore parameter folder on Gigasax, \\gigasax\DMS_Parameter_Files\AScore

            Dim paramFileFolder = m_jobParams.GetJobParameter("ParamFileStoragePath", "\\gigasax\DMS_Parameter_Files\AScore")
            success = RetrieveFile(paramFileName, paramFileFolder, 2, clsLogTools.LogLevels.ERROR)
        End If

        If success Then
            paramFilesCopied += 1
        End If

        Return success

    End Function

    <Obsolete>
    Protected Function GetDatasetID(DatasetName As String) As String
        Dim DatasetID = 0

        If m_jobParams.DatasetInfoList.TryGetValue(DatasetName, DatasetID) Then
            Return DatasetID.ToString()
        Else
            Return String.Empty
        End If

    End Function

End Class
