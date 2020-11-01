#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QTcpSocket>
#include <popup.h>
#include <QMessageBox>
#include <QInputDialog>
#include "rwidget.h"

QT_BEGIN_NAMESPACE
namespace Ui { class MainWindow; }
QT_END_NAMESPACE

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();
    enum Buttons{
        SIGN_UP1,
        SIGN_UP2,
        SIGN_IN,
        SIGN_OUT,
        INIT_STORAGE,
        CREATE_FILE,
        CREATE_FOLDER,
        UPLOAD_FILE,
        GO_BACK,
    };
    enum Commands{
        CMD_SIGN_OUT=0,
        CMD_SIGN_UP=1,
        CMD_SIGN_IN=2,
        CMD_INIT_STORAGE=3,
        CMD_CREATE_FILE=4,
        CMD_READ_FILE=5,
        CMD_UPLOAD_FILE=6,
        CMD_FILE_DELETE=7,
        CMD_FILE_INFO = 8,
        CMD_FILE_COPY = 9,
        CMD_FILE_MOVE = 0x0A,
        CMD_READ_FOLDER = 0x0B,
        CMD_CREATE_FOLDER=0x0C,
        CMD_DELETE_FOLDER=0x0D,

    };
    std::map<int, QString> errorMessages = {
        {0x10, "Authorization error!\nUnknown authorization error!"},
        {0x11, "Authorization error!\nUsername already registered!"},
        {0x12, "Authorization error!\nInvalid username during registration!"},
        {0x13, "Authorization error!\nUser does not exists!"},
        {0x14, "Authorization error!\nIncorrect password!"},
        {0x15, "Authorization error!\nInvalid token!"},
        {0x20, "File error!\nUnknown file error!"},
        {0x21, "File error!\nFile does not exist!"},
        {0x22, "File error!\nFile already exists!"},
        {0x23, "File error!\nNot enough space to create this file!"},
        {0x24, "File error!\nProhibited filename!"},
        {0x30, "Directory error!\nUnknown directory error!"},
        {0x31, "Directory error!\nDirectory does not exist!"},
        {0x32, "Directory error!\nDirectory already exists!"},
        {0x33, "Directory error!\nProhibited directory name!"},
        {0x80, "Unknown server error!"},
        {0x81, "Wrong request id!"},

    };


private:
    void changeDir(QString nextDir, bool nextFlag, bool readDir);
    int bytesToInt(QByteArray array);
    QVBoxLayout * layout;
    QSpacerItem * spacer;
    void addFile(QString name, bool folderFlag);
    void makePopup(QString text);
    void makePopup(unsigned char errorCode);
    PopUp *popUp;
    int TOKEN_SIZE=32;
    Ui::MainWindow *ui;
    bool systemSignIn(QString login, QString password);
    bool systemSignOut();
    bool systemSignUp(QString login, QString password);
    bool systemInitStorage();
    bool systemCreateFile(QString filename);
    bool systemCreateFolder(QString foldername);
    bool systemUploadFile(QString filename);
    void hideOnSign(bool signedIn);
    void clearData();
    QByteArray token;
    QTcpSocket *socket;
    QString username = "";
    QString hostname = "";
    std::vector<QWidget *> files = std::vector<QWidget *>();

    QByteArray * extendSizeToN(QByteArray array, int n);
    std::map<std::string, QByteArray> sendDataToHost(QByteArray data);
    std::map<std::string, QByteArray> sendDataToStorageServer(QString server, int port,
                                                              std::map<std::string, QByteArray> data, int type);

    QString currentFolder="";
    RWidget * currentWidget;

public slots:
    void handleButtons(int button);
    void customMenuRequested(RWidget * widget);
    void getFileInfo();
    void deleteFile();
    void copyFile();
    void openFile();
    void openFolder();
    void deleteFolder();
    void moveFile();
};
#endif // MAINWINDOW_H
