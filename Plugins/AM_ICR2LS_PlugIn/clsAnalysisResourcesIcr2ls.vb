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

		Dim RawDataType As String

		Dim strLocalDatasetFolderPath As String
		Dim strRemoteDatasetFolderPath As String
		Dim SerFileOrFolderPath As String

		Dim blnIsFolder As Boolean

		RawDataType = m_jobParams.GetParam("RawDataType")

		strRemoteDatasetFolderPath = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"))

		If RawDataType.ToLower() = RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
			strLocalDatasetFolderPath = Path.Combine(m_WorkingDir, m_DatasetName & ".d")
			strRemoteDatasetFolderPath = Path.Combine(strRemoteDatasetFolderPath, m_DatasetName & ".d")
		Else
			strLocalDatasetFolderPath = String.Copy(m_WorkingDir)
		End If

		SerFileOrFolderPath = FindSerFileOrFolder(strLocalDatasetFolderPath, blnIsFolder)

		If String.IsNullOrEmpty(SerFileOrFolderPath) Then
			' Ser file, fid file, or 0.ser folder not found in the working directory
			' See if the file exists in the archive			

			SerFileOrFolderPath = FindSerFileOrFolder(strRemoteDatasetFolderPath, blnIsFolder)

			If Not String.IsNullOrEmpty(SerFileOrFolderPath) Then
				' File found in the archive; need to copy it locally

				Dim dtStartTime As System.DateTime = System.DateTime.UtcNow

				If blnIsFolder Then
					Dim diSourceFolder As DirectoryInfo
					diSourceFolder = New DirectoryInfo(SerFileOrFolderPath)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying 0.ser folder from archive to working directory: " & SerFileOrFolderPath)
					ResetTimestampForQueueWaitTimeLogging()
					m_FileTools.CopyDirectory(SerFileOrFolderPath, Path.Combine(strLocalDatasetFolderPath, diSourceFolder.Name))

					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Successfully copied 0.ser folder in " & System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
					End If

				Else
					Dim fiSourceFile As FileInfo
					fiSourceFile = New FileInfo(SerFileOrFolderPath)

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying " & Path.GetFileName(SerFileOrFolderPath) & " file from archive to working directory: " & SerFileOrFolderPath)

					If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, strLocalDatasetFolderPath, clsLogTools.LogLevels.ERROR) Then
						Return False
					Else
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Successfully copied " & Path.GetFileName(SerFileOrFolderPath) & " file in " & System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
						End If
					End If

				End If
			End If

		End If

		Return True

	End Function

	''' <summary>
	''' Looks for a ser file, fid file, or 0.ser folder in strFolderToCheck
	''' </summary>
	''' <param name="strFolderToCheck"></param>
	''' <param name="blnIsFolder"></param>
	''' <returns>The path to the ser file, fid file, or 0.ser folder, if found.  An empty string if not found</returns>
	''' <remarks></remarks>
	Public Shared Function FindSerFileOrFolder(ByVal strFolderToCheck As String, ByRef blnIsFolder As Boolean) As String

		Dim SerFileOrFolderPath As String

		blnIsFolder = False

		' Look for a ser file in the working directory
		SerFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_SER_FILE)

		If File.Exists(SerFileOrFolderPath) Then
			' Ser file found
			Return SerFileOrFolderPath
		End If

		' Ser file not found; look for a fid file
		SerFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_FID_FILE)

		If File.Exists(SerFileOrFolderPath) Then
			' Fid file found
			Return SerFileOrFolderPath
		End If

		' Fid file not found; look for a 0.ser folder in the working directory
		SerFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_ZERO_SER_FOLDER)
		If Directory.Exists(SerFileOrFolderPath) Then
			blnIsFolder = True
			Return SerFileOrFolderPath
		End If

		Return String.Empty

	End Function

#End Region

End Class
