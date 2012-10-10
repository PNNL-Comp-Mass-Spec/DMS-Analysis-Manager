namespace MageRawSQL {
    partial class Form1 {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.listDisplayControl1 = new MageClientLib.ListDisplayControl();
            this.rawSQLPanel1 = new MageClientLib.RawSQLPanel();
            this.panel1 = new System.Windows.Forms.Panel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.sqLiteOutputPanel1 = new MageClientLib.SQLiteOutputPanel();
            this.panel2 = new System.Windows.Forms.Panel();
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.tabPage1 = new System.Windows.Forms.TabPage();
            this.tabPage2 = new System.Windows.Forms.TabPage();
            this.rawSprocPanel1 = new MageClientLib.RawSprocPanel();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.tabControl1.SuspendLayout();
            this.tabPage1.SuspendLayout();
            this.tabPage2.SuspendLayout();
            this.SuspendLayout();
            // 
            // listDisplayControl1
            // 
            this.listDisplayControl1.Accumulator = null;
            this.listDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl1.Location = new System.Drawing.Point(5, 5);
            this.listDisplayControl1.Name = "listDisplayControl1";
            this.listDisplayControl1.Notice = "";
            this.listDisplayControl1.PageTitle = "Title";
            this.listDisplayControl1.Size = new System.Drawing.Size(793, 383);
            this.listDisplayControl1.TabIndex = 1;
            // 
            // rawSQLPanel1
            // 
            this.rawSQLPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawSQLPanel1.Location = new System.Drawing.Point(3, 3);
            this.rawSQLPanel1.Name = "rawSQLPanel1";
            this.rawSQLPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.rawSQLPanel1.Size = new System.Drawing.Size(789, 124);
            this.rawSQLPanel1.TabIndex = 0;
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.statusPanel1);
            this.panel1.Controls.Add(this.sqLiteOutputPanel1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel1.Location = new System.Drawing.Point(0, 549);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(803, 109);
            this.panel1.TabIndex = 6;
            // 
            // statusPanel1
            // 
            this.statusPanel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.statusPanel1.Location = new System.Drawing.Point(0, 67);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(803, 42);
            this.statusPanel1.TabIndex = 1;
            // 
            // sqLiteOutputPanel1
            // 
            this.sqLiteOutputPanel1.DatabaseName = "C:\\Data\\test.db";
            this.sqLiteOutputPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.sqLiteOutputPanel1.Location = new System.Drawing.Point(0, 0);
            this.sqLiteOutputPanel1.Name = "sqLiteOutputPanel1";
            this.sqLiteOutputPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.sqLiteOutputPanel1.Size = new System.Drawing.Size(803, 70);
            this.sqLiteOutputPanel1.TabIndex = 4;
            this.sqLiteOutputPanel1.TableName = "DMS_Factors";
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.listDisplayControl1);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel2.Location = new System.Drawing.Point(0, 156);
            this.panel2.Name = "panel2";
            this.panel2.Padding = new System.Windows.Forms.Padding(5);
            this.panel2.Size = new System.Drawing.Size(803, 393);
            this.panel2.TabIndex = 7;
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.tabPage1);
            this.tabControl1.Controls.Add(this.tabPage2);
            this.tabControl1.Dock = System.Windows.Forms.DockStyle.Top;
            this.tabControl1.Location = new System.Drawing.Point(0, 0);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(803, 156);
            this.tabControl1.TabIndex = 8;
            // 
            // tabPage1
            // 
            this.tabPage1.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage1.Controls.Add(this.rawSQLPanel1);
            this.tabPage1.Location = new System.Drawing.Point(4, 22);
            this.tabPage1.Name = "tabPage1";
            this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage1.Size = new System.Drawing.Size(795, 130);
            this.tabPage1.TabIndex = 0;
            this.tabPage1.Tag = "Query";
            this.tabPage1.Text = "SQL Query";
            // 
            // tabPage2
            // 
            this.tabPage2.BackColor = System.Drawing.SystemColors.Control;
            this.tabPage2.Controls.Add(this.rawSprocPanel1);
            this.tabPage2.Location = new System.Drawing.Point(4, 22);
            this.tabPage2.Name = "tabPage2";
            this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
            this.tabPage2.Size = new System.Drawing.Size(795, 130);
            this.tabPage2.TabIndex = 1;
            this.tabPage2.Tag = "SprocQuery";
            this.tabPage2.Text = "Sproc Query";
            // 
            // rawSprocPanel1
            // 
            this.rawSprocPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawSprocPanel1.Location = new System.Drawing.Point(3, 3);
            this.rawSprocPanel1.Name = "rawSprocPanel1";
            this.rawSprocPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.rawSprocPanel1.Size = new System.Drawing.Size(789, 124);
            this.rawSprocPanel1.TabIndex = 0;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(803, 658);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.tabControl1);
            this.Name = "Form1";
            this.Text = "Mage - Raw SQL";
            this.panel1.ResumeLayout(false);
            this.panel2.ResumeLayout(false);
            this.tabControl1.ResumeLayout(false);
            this.tabPage1.ResumeLayout(false);
            this.tabPage2.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private MageClientLib.RawSQLPanel rawSQLPanel1;
        private MageClientLib.ListDisplayControl listDisplayControl1;
        private System.Windows.Forms.Panel panel1;
        private MageClientLib.StatusPanel statusPanel1;
        private MageClientLib.SQLiteOutputPanel sqLiteOutputPanel1;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage tabPage1;
        private System.Windows.Forms.TabPage tabPage2;
        private MageClientLib.RawSprocPanel rawSprocPanel1;
    }
}

