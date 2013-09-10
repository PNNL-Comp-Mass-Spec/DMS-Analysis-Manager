' Last modified 06/11/2009 JDS - Added logging using log4net
Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesIcr2ls
    Inherits clsAnalysisResources

#Region "Methods"
    Public Overrides Function GetResources() As IJobParams.CloseOutType
        'Retrieve param file
        If Not RetrieveFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Get input data file
		If Not RetrieveSpectra(m_jobParams.GetParam("RawDataType")) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' NOTE: GetBrukerSerFile is not MyEMSL-compatible
		If Not GetBrukerSerFile() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function GetBrukerSerFile() As Boolean

		Dim DatasetName As String
		Dim RawDataType As String

		Dim strLocalDatasetFolderPath As String
		Dim strRemoteDatasetFolderPath As String
		Dim SerFileOrFolderPath As String

		Dim blnIsFolder As Boolean

		DatasetName = m_jobParams.GetParam("DatasetNum")
		RawDataType = m_jobParams.GetParam("RawDataType")

		strRemoteDatasetFolderPath = System.IO.Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"))

		If RawDataType.ToLower() = clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
			strLocalDatasetFolderPath = System.IO.Path.Combine(m_WorkingDir, DatasetName & ".d")
			strRemoteDatasetFolderPath = System.IO.Path.Combine(strRemoteDatasetFolderPath, DatasetName & ".d")
		Else
			strLocalDatasetFolderPath = String.Copy(m_WorkingDir)
		End If

		SerFileOrFolderPath = clsAnalysisResourcesIcr2ls.FindSerFileOrFolder(strLocalDatasetFolderPath, blnIsFolder)

		If String.IsNullOrEmpty(SerFileOrFolderPath) Then
			' Ser file or 0.ser folder not found in the working directory
			' See if the file exists in the archive			

			SerFileOrFolderPath = clsAnalysisResourcesIcr2ls.FindSerFileOrFolder(strRemoteDatasetFolderPath, blnIsFolder)

			If Not String.IsNullOrEmpty(SerFileOrFolderPath) Then
				' File found in the archive; need to copy it locally

				Dim dtStartTime As System.DateTime = System.DateTime.UtcNow

				If blnIsFolder Then
					Dim diSourceFolder As System.IO.DirectoryInfo
					diSourceFolder = New System.IO.DirectoryInfo(SerFileOrFolderPath)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying 0.ser folder from archive to working directory: " & SerFileOrFolderPath)
					ResetTimestampForQueueWaitTimeLogging()
					m_FileTools.CopyDirectory(SerFileOrFolderPath, System.IO.Path.Combine(strLocalDatasetFolderPath, diSourceFolder.Name))

					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Successfully copied 0.ser folder in " & System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
					End If

				Else
					Dim fiSourceFile As System.IO.FileInfo
					fiSourceFile = New System.IO.FileInfo(SerFileOrFolderPath)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying ser file from archive to working directory: " & SerFileOrFolderPath)

					If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, strLocalDatasetFolderPath, clsLogTools.LogLevels.ERROR) Then
						Return False
					Else
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Successfully copied ser file in " & System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
						End If
					End If

				End If
			End If

		End If

		Return True

	End Function

	''' <summary>
	''' Looks for a ser file or 0.ser folder in strFolderToCheck
	''' </summary>
	''' <param name="strFolderToCheck"></param>
	''' <param name="blnIsFolder"></param>
	''' <returns>The path to the ser file or 0.ser folder, if found.  An empty string if not found</returns>
	''' <remarks></remarks>
	Public Shared Function FindSerFileOrFolder(ByVal strFolderToCheck As String, ByRef blnIsFolder As Boolean) As String

		Dim SerFileOrFolderPath As String

		blnIsFolder = False

		' Look for a ser file in the working directory
		SerFileOrFolderPath = System.IO.Path.Combine(strFolderToCheck, clsAnalysisResources.BRUKER_SER_FILE)

		If System.IO.File.Exists(SerFileOrFolderPath) Then
			' Ser file found
			Return SerFileOrFolderPath
		Else
			' Ser file not found; look for a 0.ser folder in the working directory
			' Look for the "0.ser" folder in the working directory
			SerFileOrFolderPath = System.IO.Path.Combine(strFolderToCheck, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER)
			If System.IO.Directory.Exists(SerFileOrFolderPath) Then
				blnIsFolder = True
				Return SerFileOrFolderPath
			End If
		End If

		Return String.Empty

	End Function

#End Region

End Class
