Option Explicit On 
Option Strict On

' This class can be used to search a list of values for a given value, plus or minus a given tolerance
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2005, Battelle Memorial Institute.  All Rights Reserved.
' Started November 16, 2003
'
' Last modified January 24, 2004

Friend Class clsSearchRange

    Public Sub New()
        mDataType = eDataTypeToUse.NoDataPresent
        ClearUnusedData()
    End Sub

    Private Enum eDataTypeToUse
        NoDataPresent = 0
        IntegerType = 1
        SingleType = 2
        DoubleType = 3
    End Enum

    Private mDataType As eDataTypeToUse

    Private intData() As Integer
    Private sngData() As Single
    Private dblData() As Double

    Private Sub ClearUnusedData()
        If mDataType <> eDataTypeToUse.IntegerType Then ReDim intData(0)
        If mDataType <> eDataTypeToUse.SingleType Then ReDim sngData(0)
        If mDataType <> eDataTypeToUse.DoubleType Then ReDim dblData(0)
    End Sub

    Public Function FillWithData(ByRef intValues() As Integer) As Boolean

        Dim blnSuccess As Boolean

        Try
            If intValues Is Nothing OrElse intValues.Length = 0 Then
                blnSuccess = False
            Else
                ReDim intData(intValues.Length - 1)
                intValues.CopyTo(intData, 0)

                mDataType = eDataTypeToUse.IntegerType
                blnSuccess = True
            End If
        Catch ex As Exception
            blnSuccess = False
        End Try

        If blnSuccess Then ClearUnusedData()
        Return blnSuccess
    End Function

    Public Function FillWithData(ByRef sngValues() As Single) As Boolean

        Dim blnSuccess As Boolean

        Try
            If sngValues Is Nothing OrElse sngValues.Length = 0 Then
                blnSuccess = False
            Else
                ReDim sngData(sngValues.Length - 1)
                sngValues.CopyTo(sngData, 0)

                mDataType = eDataTypeToUse.SingleType
                blnSuccess = True
            End If
        Catch ex As Exception
            blnSuccess = False
        End Try

        If blnSuccess Then ClearUnusedData()
        Return blnSuccess
    End Function

    Public Function FillWithData(ByRef dblValues() As Double) As Boolean

        Dim blnSuccess As Boolean

        Try
            If dblValues Is Nothing OrElse dblValues.Length = 0 Then
                blnSuccess = False
            Else
                ReDim dblData(dblValues.Length - 1)
                dblValues.CopyTo(dblData, 0)

                mDataType = eDataTypeToUse.DoubleType
                blnSuccess = True
            End If
        Catch ex As Exception
            blnSuccess = False
        End Try

        If blnSuccess Then ClearUnusedData()
        Return blnSuccess
    End Function

    Private Sub BinarySearchRangeInt(ByVal intSearchValue As Integer, ByVal intToleranceHalfWidth As Integer, ByRef intMatchIndexStart As Integer, ByRef intMatchIndexEnd As Integer)
        ' Recursive search function

        Dim intIndexMidpoint As Integer
        Dim blnLeftDone As Boolean
        Dim blnRightDone As Boolean
        Dim intLeftIndex As Integer
        Dim intRightIndex As Integer

        intIndexMidpoint = (intMatchIndexStart + intMatchIndexEnd) \ 2
        If intIndexMidpoint = intMatchIndexStart Then
            ' Min and Max are next to each other
            If Math.Abs(intSearchValue - intData(intMatchIndexStart)) > intToleranceHalfWidth Then intMatchIndexStart = intMatchIndexEnd
            If Math.Abs(intSearchValue - intData(intMatchIndexEnd)) > intToleranceHalfWidth Then intMatchIndexEnd = intIndexMidpoint
            Exit Sub
        End If

        If intData(intIndexMidpoint) > intSearchValue + intToleranceHalfWidth Then
            ' Out of range on the right
            intMatchIndexEnd = intIndexMidpoint
            BinarySearchRangeInt(intSearchValue, intToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        ElseIf intData(intIndexMidpoint) < intSearchValue - intToleranceHalfWidth Then
            ' Out of range on the left
            intMatchIndexStart = intIndexMidpoint
            BinarySearchRangeInt(intSearchValue, intToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        Else
            ' Inside range; figure out the borders
            intLeftIndex = intIndexMidpoint
            Do
                intLeftIndex = intLeftIndex - 1
                If intLeftIndex < intMatchIndexStart Then
                    blnLeftDone = True
                Else
                    If Math.Abs(intSearchValue - intData(intLeftIndex)) > intToleranceHalfWidth Then blnLeftDone = True
                End If
            Loop While Not blnLeftDone
            intRightIndex = intIndexMidpoint

            Do
                intRightIndex = intRightIndex + 1
                If intRightIndex > intMatchIndexEnd Then
                    blnRightDone = True
                Else
                    If Math.Abs(intSearchValue - intData(intRightIndex)) > intToleranceHalfWidth Then blnRightDone = True
                End If
            Loop While Not blnRightDone

            intMatchIndexStart = intLeftIndex + 1
            intMatchIndexEnd = intRightIndex - 1
        End If

    End Sub

    Private Sub BinarySearchRangeSng(ByVal sngSearchValue As Single, ByVal sngToleranceHalfWidth As Single, ByRef intMatchIndexStart As Integer, ByRef intMatchIndexEnd As Integer)
        ' Recursive search function

        Dim intIndexMidpoint As Integer
        Dim blnLeftDone As Boolean
        Dim blnRightDone As Boolean
        Dim intLeftIndex As Integer
        Dim intRightIndex As Integer

        intIndexMidpoint = (intMatchIndexStart + intMatchIndexEnd) \ 2
        If intIndexMidpoint = intMatchIndexStart Then
            ' Min and Max are next to each other
            If Math.Abs(sngSearchValue - sngData(intMatchIndexStart)) > sngToleranceHalfWidth Then intMatchIndexStart = intMatchIndexEnd
            If Math.Abs(sngSearchValue - sngData(intMatchIndexEnd)) > sngToleranceHalfWidth Then intMatchIndexEnd = intIndexMidpoint
            Exit Sub
        End If

        If sngData(intIndexMidpoint) > sngSearchValue + sngToleranceHalfWidth Then
            ' Out of range on the right
            intMatchIndexEnd = intIndexMidpoint
            BinarySearchRangeSng(sngSearchValue, sngToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        ElseIf sngData(intIndexMidpoint) < sngSearchValue - sngToleranceHalfWidth Then
            ' Out of range on the left
            intMatchIndexStart = intIndexMidpoint
            BinarySearchRangeSng(sngSearchValue, sngToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        Else
            ' Inside range; figure out the borders
            intLeftIndex = intIndexMidpoint
            Do
                intLeftIndex = intLeftIndex - 1
                If intLeftIndex < intMatchIndexStart Then
                    blnLeftDone = True
                Else
                    If Math.Abs(sngSearchValue - sngData(intLeftIndex)) > sngToleranceHalfWidth Then blnLeftDone = True
                End If
            Loop While Not blnLeftDone
            intRightIndex = intIndexMidpoint

            Do
                intRightIndex = intRightIndex + 1
                If intRightIndex > intMatchIndexEnd Then
                    blnRightDone = True
                Else
                    If Math.Abs(sngSearchValue - sngData(intRightIndex)) > sngToleranceHalfWidth Then blnRightDone = True
                End If
            Loop While Not blnRightDone

            intMatchIndexStart = intLeftIndex + 1
            intMatchIndexEnd = intRightIndex - 1
        End If

    End Sub

    Private Sub BinarySearchRangeDbl(ByVal dblSearchValue As Double, ByVal dblToleranceHalfWidth As Double, ByRef intMatchIndexStart As Integer, ByRef intMatchIndexEnd As Integer)
        ' Recursive search function

        Dim intIndexMidpoint As Integer
        Dim blnLeftDone As Boolean
        Dim blnRightDone As Boolean
        Dim intLeftIndex As Integer
        Dim intRightIndex As Integer

        intIndexMidpoint = (intMatchIndexStart + intMatchIndexEnd) \ 2
        If intIndexMidpoint = intMatchIndexStart Then
            ' Min and Max are next to each other
            If Math.Abs(dblSearchValue - dblData(intMatchIndexStart)) > dblToleranceHalfWidth Then intMatchIndexStart = intMatchIndexEnd
            If Math.Abs(dblSearchValue - dblData(intMatchIndexEnd)) > dblToleranceHalfWidth Then intMatchIndexEnd = intIndexMidpoint
            Exit Sub
        End If

        If dblData(intIndexMidpoint) > dblSearchValue + dblToleranceHalfWidth Then
            ' Out of range on the right
            intMatchIndexEnd = intIndexMidpoint
            BinarySearchRangeDbl(dblSearchValue, dblToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        ElseIf dblData(intIndexMidpoint) < dblSearchValue - dblToleranceHalfWidth Then
            ' Out of range on the left
            intMatchIndexStart = intIndexMidpoint
            BinarySearchRangeDbl(dblSearchValue, dblToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
        Else
            ' Inside range; figure out the borders
            intLeftIndex = intIndexMidpoint
            Do
                intLeftIndex = intLeftIndex - 1
                If intLeftIndex < intMatchIndexStart Then
                    blnLeftDone = True
                Else
                    If Math.Abs(dblSearchValue - dblData(intLeftIndex)) > dblToleranceHalfWidth Then blnLeftDone = True
                End If
            Loop While Not blnLeftDone
            intRightIndex = intIndexMidpoint

            Do
                intRightIndex = intRightIndex + 1
                If intRightIndex > intMatchIndexEnd Then
                    blnRightDone = True
                Else
                    If Math.Abs(dblSearchValue - dblData(intRightIndex)) > dblToleranceHalfWidth Then blnRightDone = True
                End If
            Loop While Not blnRightDone

            intMatchIndexStart = intLeftIndex + 1
            intMatchIndexEnd = intRightIndex - 1
        End If

    End Sub

    Public Function FindValueRange(ByVal intSearchValue As Integer, ByVal intToleranceHalfWidth As Integer, Optional ByRef intMatchIndexStart As Integer = 0, Optional ByRef intMatchIndexEnd As Integer = 0) As Boolean
        ' Searches the loaded data for sngSearchValue with a tolerance of +-sngTolerance
        ' Returns True if a match is found; in addition, populates intMatchIndexStart and intMatchIndexEnd
        ' Otherwise, returns false

        Dim blnMatchFound As Boolean

        If mDataType <> eDataTypeToUse.IntegerType Then
            Select Case mDataType
                Case eDataTypeToUse.SingleType
                    blnMatchFound = FindValueRange(CSng(intSearchValue), CSng(intToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case eDataTypeToUse.DoubleType
                    blnMatchFound = FindValueRange(CDbl(intSearchValue), CDbl(intToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case Else
                    blnMatchFound = False
            End Select
        Else
            intMatchIndexStart = 0
            intMatchIndexEnd = intData.Length - 1

            If intData.Length = 0 Then
                intMatchIndexEnd = -1
            ElseIf intData.Length = 1 Then
                If Math.Abs(intSearchValue - intData(0)) > intToleranceHalfWidth Then
                    ' Only one data point, and it is not within tolerance
                    intMatchIndexEnd = -1
                End If
            Else
                BinarySearchRangeInt(intSearchValue, intToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
            End If

            If intMatchIndexStart > intMatchIndexEnd Then
                intMatchIndexStart = -1
                intMatchIndexEnd = -1
                blnMatchFound = False
            Else
                blnMatchFound = True
            End If
        End If

        Return blnMatchFound
    End Function

    Public Function FindValueRange(ByVal dblSearchValue As Double, ByVal dblToleranceHalfWidth As Double, Optional ByRef intMatchIndexStart As Integer = 0, Optional ByRef intMatchIndexEnd As Integer = 0) As Boolean
        ' Searches the loaded data for sngSearchValue with a tolerance of +-sngTolerance
        ' Returns True if a match is found; in addition, populates intMatchIndexStart and intMatchIndexEnd
        ' Otherwise, returns false

        Dim blnMatchFound As Boolean

        If mDataType <> eDataTypeToUse.DoubleType Then
            Select Case mDataType
                Case eDataTypeToUse.IntegerType
                    blnMatchFound = FindValueRange(CInt(dblSearchValue), CInt(dblToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case eDataTypeToUse.SingleType
                    blnMatchFound = FindValueRange(CSng(dblSearchValue), CSng(dblToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case Else
                    blnMatchFound = False
            End Select
        Else
            intMatchIndexStart = 0
            intMatchIndexEnd = dblData.Length - 1

            If dblData.Length = 0 Then
                intMatchIndexEnd = -1
            ElseIf dblData.Length = 1 Then
                If Math.Abs(dblSearchValue - dblData(0)) > dblToleranceHalfWidth Then
                    ' Only one data point, and it is not within tolerance
                    intMatchIndexEnd = -1
                End If
            Else
                BinarySearchRangeDbl(dblSearchValue, dblToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
            End If

            If intMatchIndexStart > intMatchIndexEnd Then
                intMatchIndexStart = -1
                intMatchIndexEnd = -1
                blnMatchFound = False
            Else
                blnMatchFound = True
            End If
        End If

        Return blnMatchFound
    End Function

    Public Function FindValueRange(ByVal sngSearchValue As Single, ByVal sngToleranceHalfWidth As Single, Optional ByRef intMatchIndexStart As Integer = 0, Optional ByRef intMatchIndexEnd As Integer = 0) As Boolean
        ' Searches the loaded data for sngSearchValue with a tolerance of +-sngTolerance
        ' Returns True if a match is found; in addition, populates intMatchIndexStart and intMatchIndexEnd
        ' Otherwise, returns false

        Dim blnMatchFound As Boolean

        If mDataType <> eDataTypeToUse.SingleType Then
            Select Case mDataType
                Case eDataTypeToUse.IntegerType
                    blnMatchFound = FindValueRange(CInt(sngSearchValue), CInt(sngToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case eDataTypeToUse.DoubleType
                    blnMatchFound = FindValueRange(CDbl(sngSearchValue), CDbl(sngToleranceHalfWidth), intMatchIndexStart, intMatchIndexEnd)
                Case Else
                    blnMatchFound = False
            End Select
        Else
            intMatchIndexStart = 0
            intMatchIndexEnd = sngData.Length - 1

            If sngData.Length = 0 Then
                intMatchIndexEnd = -1
            ElseIf sngData.Length = 1 Then
                If Math.Abs(sngSearchValue - sngData(0)) > sngToleranceHalfWidth Then
                    ' Only one data point, and it is not within tolerance
                    intMatchIndexEnd = -1
                End If
            Else
                BinarySearchRangeSng(sngSearchValue, sngToleranceHalfWidth, intMatchIndexStart, intMatchIndexEnd)
            End If

            If intMatchIndexStart > intMatchIndexEnd Then
                intMatchIndexStart = -1
                intMatchIndexEnd = -1
                blnMatchFound = False
            Else
                blnMatchFound = True
            End If
        End If

        Return blnMatchFound
    End Function

    Public Function GetValueByIndexInt(ByVal intIndex As Integer) As Integer
        Try
            Return CInt(GetValueByIndex(intIndex))
        Catch ex As Exception
            Return 0
        End Try
    End Function

    Public Function GetValueByIndex(ByVal intIndex As Integer) As Double
        Try
            If mDataType = eDataTypeToUse.NoDataPresent Then
                Return 0
            Else
                Select Case mDataType
                    Case eDataTypeToUse.IntegerType
                        Return intData(intIndex)
                    Case eDataTypeToUse.SingleType
                        Return sngData(intIndex)
                    Case eDataTypeToUse.DoubleType
                        Return dblData(intIndex)
                End Select
            End If
        Catch ex As Exception
            ' intIndex is probably out of range
            Return 0
        End Try
    End Function

    Public Function GetValueByIndexSng(ByVal intIndex As Integer) As Single
        Try
            Return CSng(GetValueByIndex(intIndex))
        Catch ex As Exception
            Return 0
        End Try
    End Function

End Class
