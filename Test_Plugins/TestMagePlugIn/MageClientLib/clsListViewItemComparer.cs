using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MageClientLib {
    class ListViewItemComparer : System.Collections.IComparer {
        private int m_SortCol;
        private bool m_SortAscending = true;
        private bool m_SortNumeric = false;
        private bool m_SortDate = false;

        public ListViewItemComparer() {
            m_SortCol = 0;
            m_SortAscending = true;
            m_SortNumeric = false;
            m_SortDate = false;
        }
        public ListViewItemComparer(int column) {
            m_SortCol = column;
            m_SortNumeric = false;
            m_SortDate = false;
        }
        public ListViewItemComparer(int column, bool SortAscending, bool SortNumeric, bool SortDate) {
            m_SortCol = column;
            m_SortAscending = SortAscending;
            m_SortNumeric = SortNumeric;
            m_SortDate = SortDate;
        }

        // Compares two ListViewItem rows, x, and y
        public int Compare(object x, object y) {
            int intComparisonResult = 0;
            bool StringSort = true;

            float Val1 = 0;
            float Val2 = 0;

            System.DateTime Date1 = System.DateTime.MinValue;
            System.DateTime Date2 = System.DateTime.MinValue;

            string Item1 = ((ListViewItem)x).SubItems[m_SortCol].Text;
            string Item2 = ((ListViewItem)y).SubItems[m_SortCol].Text;

            if (m_SortNumeric) {
                // User has specified that the two values should be treated as integers
                // Treat an empty cell as a value of 0
                // Otherwise, try to parse as a float; if the parse fails, then auto-treat as strings

                try {
                    StringSort = false;

                    if (Item1.Length > 0) {
                        if (!float.TryParse(Item1, out Val1))
                            // Conversion failed
                            StringSort = true;
                    }

                    if (!StringSort) {
                        if (Item2.Length > 0) {
                            if (!float.TryParse(Item2, out Val2))
                                // Conversion failed
                                StringSort = true;
                        }
                    }

                    if (!StringSort) {
                        if (Val1 > Val2)
                            intComparisonResult = 1;
                        else {
                            if (Val1 < Val2)
                                intComparisonResult = -1;
                            else
                                intComparisonResult = 0;
                        }
                    }
                } catch {
                    // Conversion or comparison error
                    // Enable string sorting
                    StringSort = true;
                }

            } else {
                if (m_SortDate) {
                    // User has specified that the two values should be treated as dates
                    // Treat an empty cell as a value of 0
                    // Otherwise, try to parse as a date; if the parse fails, then auto-treat as strings

                    try {
                        StringSort = false;

                        if (Item1.Length > 0) {
                            if (!System.DateTime.TryParse(Item1, out Date1))
                                // Conversion failed
                                StringSort = true;
                        }

                        if (!StringSort) {
                            if (Item2.Length > 0) {
                                if (!System.DateTime.TryParse(Item2, out Date2))
                                    // Conversion failed
                                    StringSort = true;
                            }
                        }

                        if (!StringSort) {
                            if (Date1 > Date2)
                                intComparisonResult = 1;
                            else {
                                if (Date1 < Date2)
                                    intComparisonResult = -1;
                                else
                                    intComparisonResult = 0;
                            }
                        }
                    } catch {
                        // Conversion or comparison error
                        // Enable string sorting
                        StringSort = true;
                    }

                }
            }

            if (StringSort) {
                intComparisonResult = String.Compare(((ListViewItem)x).SubItems[m_SortCol].Text, ((ListViewItem)y).SubItems[m_SortCol].Text);
            }

            if (!m_SortAscending) {
                // Reverse the sort by changing the sign of intComparisonResult
                if (intComparisonResult < 0)
                    intComparisonResult = -intComparisonResult;
            }

            return intComparisonResult;
        }
    }
}
