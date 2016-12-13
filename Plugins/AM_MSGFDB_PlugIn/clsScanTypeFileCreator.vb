Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices
Imports ThermoRawFileReader

Public Class clsScanTypeFileCreator

    Private mErrorMessage As String
    Private mExceptionDetails As String

    Private mScanTypeMap As Dictionary(Of Integer, String)

    Private ReadOnly mWorkDir As String
    Private ReadOnly mDatasetName As String
    Private mScanTypeFilePath As String
    Private mValidScanTypeLineCount As Integer

#Region "Properties"

    Public ReadOnly Property DatasetName As String
        Get
            Return mDatasetName
        End Get
    End Property

    Public ReadOnly Property ErrorMessage As String
        Get
            Return mErrorMessage
        End Get
    End Property

    Public ReadOnly Property ExceptionDetails As String
        Get
            Return mExceptionDetails
        End Get
    End Property

    Public ReadOnly Property ScanTypeFilePath As String
        Get
            Return mScanTypeFilePath
        End Get
    End Property

    Public ReadOnly Property ValidScanTypeLineCount As Integer
        Get
            Return mValidScanTypeLineCount
        End Get
    End Property

    Public ReadOnly Property WorkDir As String
        Get
            Return mWorkDir
        End Get
    End Property

#End Region

    Public Sub New(strWorkDirectoryPath As String, strDatasetName As String)
        mWorkDir = strWorkDirectoryPath
        mDatasetName = strDatasetName
        mErrorMessage = String.Empty
        mExceptionDetails = String.Empty
        mScanTypeFilePath = String.Empty
    End Sub


    Private Function CacheScanTypeUsingScanStatsEx(strScanStatsExFilePath As String) As Boolean

        Try
            If mScanTypeMap Is Nothing Then
                mScanTypeMap = New Dictionary(Of Integer, String)
            Else
                mScanTypeMap.Clear()
            End If

            If Not File.Exists(strScanStatsExFilePath) Then
                mErrorMessage = "_ScanStatsEx.txt file not found: " & strScanStatsExFilePath
                Return False
            End If

            Using srScanStatsExFile = New StreamReader(New FileStream(strScanStatsExFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                ' Define the default column mapping
                Dim scanNumberColIndex = 1
                Dim collisionModeColIndex = 7
                Dim scanFilterColIndex = 8

                Dim linesRead = 0
                Do While Not srScanStatsExFile.EndOfStream
                    Dim dataLine = srScanStatsExFile.ReadLine()
                    If String.IsNullOrWhiteSpace(dataLine) Then Continue Do

                    linesRead += 1
                    Dim dataColumns = dataLine.Split(ControlChars.Tab)

                    Dim firstColumnIsNumber = FirstColumnIsInteger(dataColumns)

                    If linesRead = 1 AndAlso dataColumns.Length > 0 AndAlso Not firstColumnIsNumber Then
                        ' This is a header line; define the column mapping

                        Const IS_CASE_SENSITIVE = False
                        Dim lstHeaderNames = New List(Of String) From {"ScanNumber", "Collision Mode", "Scan Filter Text"}

                        ' Keys in this dictionary are column names, values are the 0-based column index
                        Dim dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, lstHeaderNames, IS_CASE_SENSITIVE)

                        scanNumberColIndex = dctHeaderMapping("ScanNumber")
                        collisionModeColIndex = dctHeaderMapping("Collision Mode")
                        scanFilterColIndex = dctHeaderMapping("Scan Filter Text")
                        Continue Do
                    End If

                    ' Parse out the values

                    Dim scanNumber As Integer
                    If TryGetValueInt(dataColumns, scanNumberColIndex, scanNumber) Then
                        Dim strCollisionMode = String.Empty
                        Dim storeData = False

                        If TryGetValueStr(dataColumns, collisionModeColIndex, strCollisionMode) Then
                            storeData = True
                        Else
                            Dim filterText = String.Empty
                            If TryGetValueStr(dataColumns, scanFilterColIndex, filterText) Then

                                filterText = dataColumns(scanFilterColIndex)

                                ' Parse the filter text to determine scan type
                                strCollisionMode = XRawFileIO.GetScanTypeNameFromFinniganScanFilterText(filterText)

                                storeData = True
                            End If
                        End If

                        If storeData Then
                            If String.IsNullOrEmpty(strCollisionMode) Then
                                strCollisionMode = "MS"
                            ElseIf strCollisionMode = "0" Then
                                strCollisionMode = "MS"
                            End If

                            If Not mScanTypeMap.ContainsKey(scanNumber) Then
                                mScanTypeMap.Add(scanNumber, strCollisionMode)
                            End If
                        End If

                    End If

                Loop

            End Using


        Catch ex As Exception
            mErrorMessage = "Exception in CacheScanTypeUsingScanStatsEx: " & ex.GetType.Name
            mExceptionDetails = ex.Message
            Return False
        End Try

        Return True

    End Function

    Public Function CreateScanTypeFile() As Boolean

        Try
            mErrorMessage = String.Empty
            mExceptionDetails = String.Empty

            mValidScanTypeLineCount = 0

            Dim strScanStatsFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanStats.txt")
            Dim strScanStatsExFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanStatsEx.txt")

            If Not File.Exists(strScanStatsFilePath) Then
                mErrorMessage = "_ScanStats.txt file not found: " & strScanStatsFilePath
                Return False
            End If

            Dim blnDetailedScanTypesDefined = clsAnalysisResourcesMSGFDB.ValidateScanStatsFileHasDetailedScanTypes(strScanStatsFilePath)

            ' Open the input file
            Using srScanStatsFile = New StreamReader(New FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                mScanTypeFilePath = Path.Combine(mWorkDir, mDatasetName & "_ScanType.txt")

                ' Create the scan type output file
                Using swOutFile = New StreamWriter(New FileStream(mScanTypeFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                    swOutFile.WriteLine("ScanNumber" & ControlChars.Tab & "ScanTypeName" & ControlChars.Tab & "ScanType" & ControlChars.Tab & "ScanTime")

                    ' Define the default column mapping
                    Dim scanNumberColIndex = 1
                    Dim scanTimeColIndex = 2
                    Dim scanTypeColIndex = 3
                    Dim scanTypeNameColIndex = -1
                    Dim scanStatsExLoaded = False

                    Dim linesRead = 0
                    Do While Not srScanStatsFile.EndOfStream
                        Dim dataLine = srScanStatsFile.ReadLine()
                        If String.IsNullOrWhiteSpace(dataLine) Then
                            Continue Do
                        End If

                        linesRead += 1
                        Dim dataColumns = dataLine.Split(ControlChars.Tab)

                        Dim firstColumnIsNumber = FirstColumnIsInteger(dataColumns)

                        If linesRead = 1 AndAlso dataColumns.Length > 0 AndAlso Not firstColumnIsNumber Then
                            ' This is a header line; define the column mapping

                            Const IS_CASE_SENSITIVE = False
                            Dim lstHeaderNames = New List(Of String) From {"ScanNumber", "ScanTime", "ScanType", "ScanTypeName"}

                            ' Keys in this dictionary are column names, values are the 0-based column index
                            Dim dctHeaderMapping = clsGlobal.ParseHeaderLine(dataLine, lstHeaderNames, IS_CASE_SENSITIVE)

                            scanNumberColIndex = dctHeaderMapping("ScanNumber")
                            scanTimeColIndex = dctHeaderMapping("ScanTime")
                            scanTypeColIndex = dctHeaderMapping("ScanType")
                            scanTypeNameColIndex = dctHeaderMapping("ScanTypeName")
                            Continue Do
                        End If

                        If linesRead = 1 AndAlso firstColumnIsNumber AndAlso dataColumns.Length >= 11 AndAlso blnDetailedScanTypesDefined Then
                            ' This is a ScanStats file that does not have a header line
                            ' Assume the column indices are 1, 2, 3, and 10

                            scanNumberColIndex = 1
                            scanTimeColIndex = 2
                            scanTypeColIndex = 3
                            scanTypeNameColIndex = 10
                        End If

                        If scanTypeNameColIndex < 0 And Not scanStatsExLoaded Then
                            ' Need to read the ScanStatsEx file

                            If Not CacheScanTypeUsingScanStatsEx(strScanStatsExFilePath) Then
                                srScanStatsFile.Close()
                                swOutFile.Close()
                                Return False
                            End If

                            scanStatsExLoaded = True

                        End If

                        Dim scanNumber = 0
                        Dim scanType = 0
                        If Not TryGetValueInt(dataColumns, scanNumberColIndex, scanNumber) Then Continue Do

                        If Not TryGetValueInt(dataColumns, scanTypeColIndex, scanType) Then Continue Do

                        Dim scanTypeName = String.Empty
                        If scanStatsExLoaded Then
                            mScanTypeMap.TryGetValue(scanNumber, scanTypeName)
                        ElseIf scanTypeNameColIndex >= 0 Then
                            TryGetValueStr(dataColumns, scanTypeNameColIndex, scanTypeName)
                        End If

                        Dim scanTime As Single
                        TryGetValueSng(dataColumns, scanTimeColIndex, scanTime)

                        swOutFile.WriteLine(scanNumber & ControlChars.Tab &
                                            scanTypeName & ControlChars.Tab &
                                            scanType & ControlChars.Tab &
                                            scanTime.ToString("0.0000"))

                        mValidScanTypeLineCount += 1

                    Loop


                End Using

            End Using

        Catch ex As Exception
            mErrorMessage = "Exception in CreateScanTypeFile: " & ex.GetType.Name
            mExceptionDetails = ex.Message
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Return true if the value in the first index of dataColumns is an Integer
    ''' </summary>
    ''' <param name="dataColumns"></param>
    ''' <returns></returns>
    Private Function FirstColumnIsInteger(dataColumns As String()) As Boolean
        Dim dataValue As Integer
        Return Integer.TryParse(dataColumns(0), dataValue)
    End Function

    ''' <summary>
    ''' Tries to convert the text at index colIndex of strData to an integer
    ''' </summary>
    ''' <param name="dataColumns"></param>
    ''' <param name="colIndex"></param>
    ''' <param name="intValue"></param>
    ''' <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
    ''' <remarks></remarks>
    Private Function TryGetValueInt(dataColumns() As String, colIndex As Integer, <Out()> ByRef intValue As Integer) As Boolean
        If colIndex >= 0 AndAlso colIndex < dataColumns.Length Then
            If Integer.TryParse(dataColumns(colIndex), intValue) Then
                Return True
            End If
        End If

        intValue = 0
        Return False
    End Function

    ''' <summary>
    ''' Tries to convert the text at index colIndex of strData to a float
    ''' </summary>
    ''' <param name="dataColumns"></param>
    ''' <param name="colIndex"></param>
    ''' <param name="sngValue"></param>
    ''' <returns>True if success; false if colIndex is less than 0, colIndex is out of range for dataColumns(), or the text cannot be converted to an integer</returns>
    ''' <remarks></remarks>
    Private Function TryGetValueSng(dataColumns() As String, colIndex As Integer, <Out()> ByRef sngValue As Single) As Boolean
        If colIndex >= 0 AndAlso colIndex < dataColumns.Length Then
            If Single.TryParse(dataColumns(colIndex), sngValue) Then
                Return True
            End If
        End If

        sngValue = 0
        Return False
    End Function

    ''' <summary>
    ''' Tries to retrieve the string value at index colIndex in dataColumns()
    ''' </summary>
    ''' <param name="dataColumns"></param>
    ''' <param name="colIndex"></param>
    ''' <param name="strValue"></param>
    ''' <returns>True if success; false if colIndex is less than 0 or colIndex is out of range for dataColumns()</returns>
    ''' <remarks></remarks>
    Private Function TryGetValueStr(dataColumns() As String, colIndex As Integer, <Out()> ByRef strValue As String) As Boolean
        If colIndex >= 0 AndAlso colIndex < dataColumns.Length Then
            strValue = dataColumns(colIndex)
            If String.IsNullOrEmpty(strValue) Then strValue = String.Empty
            Return True
        End If

        strValue = String.Empty
        Return False
    End Function
End Class
