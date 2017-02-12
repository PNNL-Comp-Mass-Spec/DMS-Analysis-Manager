Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Threading
Imports MyEMSLReader
Imports PRISM.Processes

Public Class clsAnalysisResourcesLCMSFF
    Inherits clsAnalysisResources

    Public Const SCANS_FILE_SUFFIX As String = "_scans.csv"
    Public Const ISOS_FILE_SUFFIX As String = "_isos.csv"

    Public Overrides Function GetResources() As CloseOutType

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting required files")

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Retrieve Decon2LS _scans.csv file for this dataset
        ' The LCMSFeature Finder doesn't actually use the _scans.csv file, but we want to be sure it's present in the results folder
        Dim strFileToGet = m_DatasetName & SCANS_FILE_SUFFIX
        If Not FindAndRetrieveMiscFiles(strFileToGet, False) Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        ' Retrieve Decon2LS _isos.csv files for this dataset
        strFileToGet = m_DatasetName & ISOS_FILE_SUFFIX
        If Not FindAndRetrieveMiscFiles(strFileToGet, False) Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If
        m_jobParams.AddResultFileToSkip(strFileToGet)

        ' Retrieve the LCMSFeatureFinder .Ini file specified for this job
        Dim strLCMSFFIniFileName = m_jobParams.GetParam("LCMSFeatureFinderIniFile")
        If strLCMSFFIniFileName Is Nothing OrElse strLCMSFFIniFileName.Length = 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "LCMSFeatureFinderIniFile not defined in the settings for this job; unable to continue")
            Return CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        Dim strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "LCMSFeatureFinder"
        Dim strFFIniFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strFFIniFileStoragePath Is Nothing OrElse strFFIniFileStoragePath.Length = 0 Then
            strFFIniFileStoragePath = "\\gigasax\DMS_Parameter_Files\LCMSFeatureFinder"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strFFIniFileStoragePath)
        End If

        If Not CopyFileToWorkDir(strLCMSFFIniFileName, strFFIniFileStoragePath, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        Dim strRawDataType As String
        strRawDataType = m_jobParams.GetParam("RawDataType")
        If strRawDataType.ToLower = RAW_DATA_TYPE_DOT_UIMF_FILES Then

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving .UIMF file")
            End If


            ' IMS data; need to get the .UIMF file
            If Not RetrieveSpectra(strRawDataType) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
                Return CloseOutType.CLOSEOUT_FAILED
            Else
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieved .UIMF file")
                End If
            End If

            m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION)

        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Could add an extension of a file to delete, like this:
        ''m_JobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

        ' Customize the LCMSFeatureFinder .Ini file to include the input file path and output folder path
        Dim success = UpdateFeatureFinderIniFile(strLCMSFFIniFileName)
        If Not success Then
            Dim Msg As String = "clsAnalysisResourcesLCMSFF.GetResources(), failed customizing .Ini file " & strLCMSFFIniFileName
            If String.IsNullOrEmpty(m_message) Then
                m_message = Msg
            End If
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function GetValue(strLine As String) As String
        Dim intEqualsIndex As Integer
        Dim strValue As String = String.Empty

        If Not String.IsNullOrEmpty(strLine) Then
            intEqualsIndex = strLine.IndexOf("="c)
            If intEqualsIndex > 0 AndAlso intEqualsIndex < strLine.Length - 1 Then
                strValue = strLine.Substring(intEqualsIndex + 1)
            End If
        End If

        Return strValue

    End Function

    Protected Function UpdateFeatureFinderIniFile(strLCMSFFIniFileName As String) As Boolean

        Const INPUT_FILENAME_KEY = "InputFileName"
        Const OUTPUT_DIRECTORY_KEY = "OutputDirectory"
        Const FILTER_FILE_NAME_KEY = "DeconToolsFilterFileName"


        ' Read the source .Ini file and update the settings for InputFileName and OutputDirectory
        ' In addition, look for an entry for DeconToolsFilterFileName; 
        '  if present, verify that the file exists and copy it locally (so that it will be included in the results folder)

        Dim SrcFilePath As String = Path.Combine(m_WorkingDir, strLCMSFFIniFileName)
        Dim TargetFilePath As String = Path.Combine(m_WorkingDir, strLCMSFFIniFileName & "_new")
        Dim IsosFilePath As String = Path.Combine(m_WorkingDir, DatasetName & ISOS_FILE_SUFFIX)

        Dim strLineIn As String
        Dim strLineInLCase As String

        Dim blnInputFileDefined As Boolean
        Dim blnOutputDirectoryDefined As Boolean

        blnInputFileDefined = False
        blnOutputDirectoryDefined = False
        Dim result = True


        Try
            ' Create the output file (temporary name ending in "_new"; we'll swap the files later)
            Using swOutFile = New StreamWriter(New FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                Try
                    ' Open the input file
                    Using srInFile = New StreamReader(New FileStream(SrcFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                        Do While Not srInFile.EndOfStream
                            strLineIn = srInFile.ReadLine

                            If strLineIn Is Nothing Then Continue Do

                            strLineInLCase = strLineIn.ToLower().Trim()

                            If strLineInLCase.StartsWith(INPUT_FILENAME_KEY.ToLower) Then
                                ' Customize the input file name
                                strLineIn = INPUT_FILENAME_KEY & "=" & IsosFilePath
                                blnInputFileDefined = True
                            End If

                            If strLineInLCase.StartsWith(OUTPUT_DIRECTORY_KEY.ToLower) Then
                                ' Customize the output directory name
                                strLineIn = OUTPUT_DIRECTORY_KEY & "=" & m_WorkingDir
                                blnOutputDirectoryDefined = True
                            End If

                            If strLineInLCase.StartsWith(FILTER_FILE_NAME_KEY.ToLower) Then
                                ' Copy the file defined by DeconToolsFilterFileName= to the working directory

                                Dim strValue = GetValue(strLineIn)

                                If Not String.IsNullOrEmpty(strValue) Then
                                    Dim fiFileInfo = New FileInfo(strValue)
                                    If Not fiFileInfo.Exists Then
                                        m_message = "Entry for " & FILTER_FILE_NAME_KEY & " in " & strLCMSFFIniFileName & " points to an invalid file: " & strValue
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                                        result = False
                                        Exit Do
                                    Else
                                        ' Copy the file locally
                                        Dim strTargetFilePath As String = Path.Combine(m_WorkingDir, fiFileInfo.Name)
                                        fiFileInfo.CopyTo(strTargetFilePath)
                                    End If
                                End If

                            End If

                            swOutFile.WriteLine(strLineIn)

                        Loop

                    End Using

                    If Not blnInputFileDefined Then
                        swOutFile.WriteLine(INPUT_FILENAME_KEY & "=" & IsosFilePath)
                    End If

                    If Not blnOutputDirectoryDefined Then
                        swOutFile.WriteLine(OUTPUT_DIRECTORY_KEY & "=" & m_WorkingDir)
                    End If

                Catch ex As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" & strLCMSFFIniFileName & "): " & ex.Message)
                    result = False
                End Try

            End Using

            ' Wait 250 millseconds, then replace the original .Ini file with the new one
            Thread.Sleep(250)
            clsProgRunner.GarbageCollectNow()

            ' Delete the input file
            File.Delete(SrcFilePath)

            ' Wait another 250 milliseconds before renaming the output file
            Thread.Sleep(50)
            clsProgRunner.GarbageCollectNow()

            ' Rename the newly created output file to have the name of the input file
            File.Move(TargetFilePath, SrcFilePath)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesLCMSFF.UpdateFeatureFinderIniFile, Error opening the .Ini file to customize (" & strLCMSFFIniFileName & "): " & ex.Message)
            result = False
        End Try

        Return result
    End Function

End Class
