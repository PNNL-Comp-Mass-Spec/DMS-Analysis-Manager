Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsMODPlusResultsReader

    ''' <summary>
    ''' Data lines for the current scan
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property CurrentScanData As List(Of String)
        Get
            Return mCurrentScanData
        End Get
    End Property

    ''' <summary>
    ''' Currently available scan number and charge
    ''' For example if scan 1000 and charge 2, will be 1000.02
    ''' Or if scan 1000 and charge 4, will be 1000.04
    ''' </summary>
    ''' <remarks>-1 if no more scans remain</remarks>
    Public ReadOnly Property CurrentScanChargeCombo As Double
        Get
            Return mCurrentScanChargeCombo
        End Get
    End Property

    Public ReadOnly Property ResultFile As FileInfo
        Get
            Return mResultFile
        End Get
    End Property

    Public ReadOnly Property SpectrumAvailable As Boolean
        Get
            Return mSpectrumAvailable
        End Get
    End Property


    Protected mCurrentScanChargeCombo As Double
    Protected mCurrentScanData As List(Of String)

    Protected mSavedLine As String
    Protected mSpectrumAvailable As Boolean

    Protected ReadOnly mExtractChargeAndScan As Regex
    Protected ReadOnly mExtractFilenameWithoutPartTag As Regex

    Protected ReadOnly mReader As StreamReader
    Protected ReadOnly mResultFile As FileInfo


    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(datasetName As String, modPlusResultsFile As FileInfo)

        mResultFile = modPlusResultsFile

        ' This RegEx is used to parse out the charge and scan number from the current spectrum
        ' LineFormat (where \t is tab)
        ' >>MGFFilePath \t MGFScanIndex \t ScanNumber \t ParentMZ \t Charge \t MGFScanHeader

        ' Example lines:
        ' >>E:\DMS_WorkDir\O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28_Part4.mgf	522	0	841.5054	2	O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28.4165.4165.
        ' >>E:\DMS_WorkDir\O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28_Part4.mgf	524	0	1037.5855	2	O_disjunctus_PHG_test_01_Run2_30Dec13_Samwise_13-07-28.4181.4181.2

        ' Notice that some lines have MGFScanHeaders of 
        '   Charge<Tab>Dataset.StartScan.EndScan.
        ' while others have
        '   Charge<Tab>Dataset.StartScan.EndScan.Charge

        mExtractChargeAndScan = New Regex("\t(\d+)\t" & datasetName & "\.(\d+)\.", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        mExtractFilenameWithoutPartTag = New Regex("(.+)_Part\d+\.mgf", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        mReader = New StreamReader(New FileStream(modPlusResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

        mCurrentScanData = New List(Of String)
        mSavedLine = String.Empty

        ReadNextSpectrum()

    End Sub

    Public Function ReadNextSpectrum() As Boolean

        mSpectrumAvailable = False
        mCurrentScanChargeCombo = -1

        If mReader.EndOfStream Then
            Return False
        End If

        Dim dataLine As String
        If Not String.IsNullOrEmpty(mSavedLine) Then
            dataLine = String.Copy(mSavedLine)
            mSavedLine = String.Empty
        Else
            dataLine = mReader.ReadLine()
        End If

        mCurrentScanData.Clear()

        Dim startScanFound = False

        While True

            If Not String.IsNullOrWhiteSpace(dataLine) Then
                If dataLine.StartsWith(">>") Then
                    If startScanFound Then
                        ' This is the second time we've encountered ">>" in this function
                        ' Cache the line so it can be used the next time ReadNextSpectrum is called
                        mSavedLine = dataLine
                        mSpectrumAvailable = True
                        Return True
                    End If

                    startScanFound = True

                    Dim reMatch As Match = mExtractChargeAndScan.Match(dataLine)
                    
                    mCurrentScanChargeCombo = 0

                    Dim charge = 0
                    Dim scan = 0
                    Dim scanMatched = False

                    If reMatch.Success Then
                        Integer.TryParse(reMatch.Groups(1).Value, charge)
                        If Integer.TryParse(reMatch.Groups(2).Value, scan) Then
                            mCurrentScanChargeCombo = scan + charge / 100.0
                            scanMatched = True
                        Else
                            mCurrentScanChargeCombo = 0
                        End If

                    End If

                    ' Replace the file path in this line with a generic path of "E:\DMS_WorkDir\"
                    ' In addition, update the scan number if it is 0
                    ' And, remove "_Part#" from the filename

                    ' For example, change from
                    ' >>E:\DMS_WorkDir3\DatasetX_Part3.mgf	51	0	1481.7382	3	DatasetX.592.592.
                    ' to
                    ' >>E:\DMS_WorkDir\DatasetX.mgf	51	592	1481.7382	3	DatasetX.592.592.

                    Try

                        Dim dataColumns = dataLine.Split(ControlChars.Tab)
                        If dataColumns.Count > 3 Then

                            Dim mgfFilePath = dataColumns(0).TrimStart(">"c)
                            Dim fiMgfFileLocal = New FileInfo(mgfFilePath)

                            If fiMgfFileLocal.Name.Length > 0 Then
                                ' Reconstruct dataLine

                                Dim reNameMatch = mExtractFilenameWithoutPartTag.Match(fiMgfFileLocal.Name)
                                If reNameMatch.Success Then
                                    mgfFilePath = Path.Combine("E:\DMS_WorkDir\", reNameMatch.Groups(1).Value & fiMgfFileLocal.Extension)
                                Else
                                    mgfFilePath = Path.Combine("E:\DMS_WorkDir\", fiMgfFileLocal.Name)
                                End If

                                dataLine = ">>" & mgfFilePath & ControlChars.Tab & dataColumns(1) & ControlChars.Tab

                                If scanMatched AndAlso dataColumns(2) = "0" Then
                                    dataLine &= scan
                                Else
                                    dataLine &= dataColumns(2)
                                End If

                                ' Add the remaining columns
                                For colIndex = 3 To dataColumns.Length - 1
                                    dataLine &= ControlChars.Tab & dataColumns(colIndex)
                                Next

                            End If
                        End If

                    Catch ex As Exception
                        ' Text parsing error
                        ' Do not reconstruct dataLine
                    End Try

                End If
                mCurrentScanData.Add(dataLine)
            End If

            If mReader.EndOfStream Then
                If mCurrentScanData.Count > 0 Then
                    mSpectrumAvailable = True
                    Return True
                Else
                    Return False
                End If
            End If

            dataLine = mReader.ReadLine()

        End While

        ' ReSharper disable once VbUnreachableCode
        Return False

    End Function
End Class
