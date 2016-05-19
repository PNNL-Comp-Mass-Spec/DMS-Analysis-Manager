Option Strict On

Imports System.IO

Public Class clsScanStatsGenerator

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

    ''' <summary>
    ''' When ScanStart is > 0, will start processing at the specified scan number
    ''' </summary>
    Public Property ScanStart As Integer

    ''' <summary>
    ''' When ScanEnd is > 0, will stop processing at the specified scan number
    ''' </summary>
    Public Property ScanEnd As Integer

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="msFileInfoScannerDLLPath"></param>
    ''' <param name="debugLevel"></param>
    ''' <remarks></remarks>
    Public Sub New(msFileInfoScannerDLLPath As String, debugLevel As Integer)
        mMSFileInfoScannerDLLPath = msFileInfoScannerDLLPath
        mDebugLevel = debugLevel

        mErrorMessage = String.Empty
        ScanStart = 0
        ScanEnd = 0
    End Sub

    ''' <summary>
    ''' Create the ScanStats file for the given dataset file
    ''' </summary>
    ''' <param name="strInputFilePath">Dataset file</param>
    ''' <param name="strOutputFolderPath">Output folder</param>
    ''' <returns></returns>
    ''' <remarks>Will list DatasetID as 0 in the output file</remarks>
    Public Function GenerateScanStatsFile(strInputFilePath As String, strOutputFolderPath As String) As Boolean
        Return GenerateScanStatsFile(strInputFilePath, strOutputFolderPath, 0)
    End Function

    ''' <summary>
    ''' Create the ScanStats file for the given dataset file
    ''' </summary>
    ''' <param name="strInputFilePath">Dataset file</param>
    ''' <param name="strOutputFolderPath">Output folder</param>
    ''' <param name="intDatasetID">Dataset ID</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function GenerateScanStatsFile(strInputFilePath As String, strOutputFolderPath As String, intDatasetID As Integer) As Boolean

        Dim blnSuccess As Boolean

        Try

            mMSFileInfoScannerErrorCount = 0

            ' Initialize the MSFileScanner class					
            mMSFileInfoScanner = LoadMSFileInfoScanner(mMSFileInfoScannerDLLPath)

            mMSFileInfoScanner.CheckFileIntegrity = False
            mMSFileInfoScanner.CreateDatasetInfoFile = False
            mMSFileInfoScanner.CreateScanStatsFile = True
            mMSFileInfoScanner.SaveLCMS2DPlots = False
            mMSFileInfoScanner.SaveTICAndBPIPlots = False
            mMSFileInfoScanner.CheckCentroidingStatus = False

            mMSFileInfoScanner.UpdateDatasetStatsTextFile = False
            mMSFileInfoScanner.DatasetIDOverride = intDatasetID

            If Me.ScanStart > 0 Or Me.ScanEnd > 0 Then
                mMSFileInfoScanner.ScanStart = Me.ScanStart
                mMSFileInfoScanner.ScanEnd = Me.ScanEnd
            End If

            blnSuccess = mMSFileInfoScanner.ProcessMSFileOrFolder(strInputFilePath, strOutputFolderPath)

            If Not blnSuccess Then
                mErrorMessage = "Error generating ScanStats file using " & strInputFilePath
                Dim strMsgAddnl As String = mMSFileInfoScanner.GetErrorMessage

                If Not String.IsNullOrEmpty(strMsgAddnl) Then
                    mErrorMessage = mErrorMessage & ": " & strMsgAddnl
                End If
            End If

        Catch ex As Exception
            mErrorMessage = "Exception in GenerateScanStatsFile: " & ex.Message
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
				Dim obj As Object
				obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath)
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
			Dim assem As Reflection.Assembly
			assem = Reflection.Assembly.LoadFrom(strDLLFilePath)
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
