Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsDeconToolsQCPlotsGenerator

	Protected mDebugLevel As Integer
	Protected mErrorMessage As String
	Protected mMSFileInfoScannerDLLPath As String

	Protected WithEvents mMSFileInfoScanner As MSFileInfoScannerInterfaces.iMSFileInfoScanner
	Protected mMSFileInfoScannerErrorCount As Integer

	Public ReadOnly Property ErrorMessage As String
		Get
			Return mErrorMessage
		End Get
	End Property

	Public ReadOnly Property MSFileInfoScannerErrorCount As Integer
		Get
			Return mMSFileInfoScannerErrorCount
		End Get
	End Property

    Public Sub New(MSFileInfoScannerDLLPath As String, DebugLevel As Integer)
        mMSFileInfoScannerDLLPath = MSFileInfoScannerDLLPath
        mDebugLevel = DebugLevel

        mErrorMessage = String.Empty
    End Sub

    Public Function CreateQCPlots(strInputFilePath As String, strOutputFolderPath As String) As Boolean

        Dim blnSuccess As Boolean

        Try

            mMSFileInfoScannerErrorCount = 0

            ' Initialize the MSFileScanner class					
            mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath)
            With mMSFileInfoScanner
                .CheckFileIntegrity = False
                .CreateDatasetInfoFile = False
                .CreateScanStatsFile = False
                .SaveLCMS2DPlots = True
                .SaveTICAndBPIPlots = True
                .UpdateDatasetStatsTextFile = False
            End With

            blnSuccess = mMSFileInfoScanner.ProcessMSFileOrFolder(strInputFilePath, strOutputFolderPath)

            If Not blnSuccess Then
                mErrorMessage = "Error generating QC Plots using " & strInputFilePath
                Dim strMsgAddnl As String = mMSFileInfoScanner.GetErrorMessage

                If Not String.IsNullOrEmpty(strMsgAddnl) Then
                    mErrorMessage = mErrorMessage & ": " & strMsgAddnl
                End If
            End If

        Catch ex As Exception
            mErrorMessage = "Exception in CreateQCPlots: " & ex.Message
            Return False
        End Try

        Return blnSuccess

    End Function

	Protected Function LoadMSFileInfoScanner(strMSFileInfoScannerDLLPath As String) As MSFileInfoScannerInterfaces.iMSFileInfoScanner
		Const MsDataFileReaderClass As String = "MSFileInfoScanner.clsMSFileInfoScanner"

		Dim objMSFileInfoScanner As MSFileInfoScannerInterfaces.iMSFileInfoScanner = Nothing
		Dim msg As String

		Try
			If Not File.Exists(strMSFileInfoScannerDLLPath) Then
				msg = "DLL not found: " + strMSFileInfoScannerDLLPath
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
			Else
                Dim obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath)
				If obj IsNot Nothing Then
					objMSFileInfoScanner = DirectCast(obj, MSFileInfoScannerInterfaces.iMSFileInfoScanner)
					msg = "Loaded MSFileInfoScanner from " + strMSFileInfoScannerDLLPath
					If mDebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg)
					End If
				End If

			End If
		Catch ex As Exception
			msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
		End Try

		Return objMSFileInfoScanner
	End Function

	Protected Function LoadObject(className As String, strDLLFilePath As String) As Object
		Dim obj As Object = Nothing
		Try
			' Dynamically load the specified class from strDLLFilePath
            Dim assem = Reflection.Assembly.LoadFrom(strDLLFilePath)
			Dim dllType As Type = assem.[GetType](className, False, True)
			obj = Activator.CreateInstance(dllType)
		Catch ex As Exception
			Dim msg As String = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
		End Try
		Return obj
	End Function

	Protected Sub mMSFileInfoScanner_ErrorEvent(Message As String) Handles mMSFileInfoScanner.ErrorEvent
		mMSFileInfoScannerErrorCount += 1
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MSFileInfoScanner error: " & Message)
	End Sub

	Protected Sub mMSFileInfoScanner_MessageEvent(Message As String) Handles mMSFileInfoScanner.MessageEvent
		If mDebugLevel >= 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & Message)
		End If
	End Sub

End Class
