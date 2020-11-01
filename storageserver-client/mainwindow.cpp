#include "mainwindow.h"

#include "ui_mainwindow.h"
#include <QSignalMapper>
#include<QDebug>
#include <map>
#include <QGraphicsView>
#include <QGraphicsPixmapItem>
#include <QHostAddress>
#include <QFileDialog>
#include <QProgressDialog>

using namespace std;

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
    , ui(new Ui::MainWindow)
{
    ui->setupUi(this);
    layout = ui->files_layout;
    spacer = ui->files_spacer;

    ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
    ui->register_widget->setVisible(false);
    ui->entered_widget->setVisible(false);
    ui->register_line->setVisible(false);

    QSignalMapper *mapper = new QSignalMapper(this);


    QPushButton *sign_up1 = ui->sign_up_button1;
    connect(sign_up1, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(sign_up1, SIGN_UP1);

    QPushButton *sign_up2 = ui->sign_up_button2;
    connect(sign_up2, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(sign_up2, SIGN_UP2);

    QPushButton *sign_in = ui->sign_in_button;
    connect(sign_in, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(sign_in, SIGN_IN);

    QPushButton *sign_out = ui->sign_out_button;
    connect(sign_out, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(sign_out, SIGN_OUT);

    QPushButton *init_storage = ui->init_storage_button;
    connect(init_storage, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(init_storage, INIT_STORAGE);

    QPushButton *create_file = ui->create_file_button;
    connect(create_file, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(create_file, CREATE_FILE);

    QPushButton *create_folder = ui->create_folder_button;
    connect(create_folder, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(create_folder, CREATE_FOLDER);

    QPushButton *upload_file = ui->upload_button;
    connect(upload_file, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(upload_file, UPLOAD_FILE);

    QPushButton *back_button = ui->back_button;
    connect(back_button, SIGNAL (clicked(bool)), mapper, SLOT (map()));
    mapper->setMapping(back_button, GO_BACK);


    connect(mapper, SIGNAL(mapped(int)), this, SLOT(handleButtons(int)));

    popUp = new PopUp();

    ui->create_file_button->setEnabled(false);
    ui->create_folder_button->setEnabled(false);
    ui->init_storage_button->setEnabled(false);
    ui->upload_button->setEnabled(false);
    ui->back_button->setEnabled(false);
    ui->folder_label->setText("");




}




void MainWindow::handleButtons(int button){
    qDebug() <<button;
    if(button==SIGN_UP1){
        ui->register_widget->setVisible(true);
        ui->register_line->setVisible(true);
        ui->sign_up_button1->setVisible(false);

    }else if(button==SIGN_UP2){
        ui->sign_in_button->setEnabled(false);
        ui->sign_up_button1->setEnabled(false);
        ui->sign_up_button2->setEnabled(false);
        QString login = ui->sign_up_login->text();
        QString pass1 = ui->sign_up_pass1->text();
        QString pass2 = ui->sign_up_pass2->text();
        bool same_passwords = pass1==pass2;
        if(same_passwords){
            if(systemSignUp(login, pass1)){
                ui->line_username->setText(username);
                hideOnSign(true);
                clearData();
                ui->sign_up_button1->setVisible(true);
                changeDir("", false, true);
            }
        }else{
            makePopup("Password confirmation and password are different!");
        }
        ui->sign_in_button->setEnabled(true);
        ui->sign_up_button1->setEnabled(true);
        ui->sign_up_button2->setEnabled(true);


    }else if(button==SIGN_IN){
        QString login = ui->login_login->text();
        QString pass = ui->login_pass->text();
        ui->sign_in_button->setEnabled(false);
        ui->sign_up_button1->setEnabled(false);
        ui->sign_up_button2->setEnabled(false);
        if(systemSignIn(login, pass)){
            ui->line_username->setText(username);
            clearData();
            hideOnSign(true);
            changeDir("", false, true);
        }
        ui->sign_in_button->setEnabled(true);
        ui->sign_up_button1->setEnabled(true);
        ui->sign_up_button2->setEnabled(true);


    }else if(button==SIGN_OUT){
        ui->sign_out_button->setEnabled(false);
        if(systemSignOut()){
            ui->line_username->setText(username);
            ui->folder_label->setText("");
            hideOnSign(false);
            ui->sign_up_button1->setVisible(true);
            QLayoutItem *child;
            while(ui->files_layout->count()>1&&(child=ui->files_layout->takeAt(0))!=0){
                delete child->widget();
            }

            ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
        }
        ui->sign_out_button->setEnabled(true);

    }else if(button==INIT_STORAGE){
        ui->init_storage_button->setEnabled(false);
        QMessageBox::StandardButton reply;
        reply = QMessageBox::question(this, "Storage Initialization", "You may delete your stored data. Are you sure?",
                                   QMessageBox::Yes|QMessageBox::No);
        if (reply == QMessageBox::Yes) {
            if(systemInitStorage()){
                QLayoutItem *child;
                while(ui->files_layout->count()>1&&(child=ui->files_layout->takeAt(0))!=0){
                    delete child->widget();
                }

                ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());



            }
        }
        currentFolder="";
        changeDir("", false, true);
        ui->init_storage_button->setEnabled(true);

    }else if(button==CREATE_FILE){
        bool ok;
        QString text = QInputDialog::getText(this, "Create new file",
                                             "Filename:", QLineEdit::Normal, "", &ok, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
        if (ok){
            if(text.isEmpty()){
                makePopup("You should provide filename!");
            }else{
                systemCreateFile(text);
            }
        }
    }else if(button==CREATE_FOLDER){
        bool ok;
        QString text = QInputDialog::getText(this, "Create new folder",
                                             "Folder name:", QLineEdit::Normal, "", &ok, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
        if (ok){
            if(text.isEmpty()){
                makePopup("You should provide folder name!");
            }else{
                systemCreateFolder(text);
            }
        }
    }else if(button==UPLOAD_FILE){
        QString fileName = QFileDialog::getOpenFileName(this,
                tr("Upload file"), "",
                tr("All Files (*)"));
        if(systemUploadFile(fileName)){
            addFile(fileName.split("/").last(), false);
        }
    }else if(button==GO_BACK){
        changeDir("", false, true);
    }
}

void MainWindow::hideOnSign(bool signedIn){
    ui->login_widget->setVisible(!signedIn);
    ui->host_widget->setVisible(!signedIn);
    ui->line_host->setVisible(!signedIn);


    ui->entered_widget->setVisible(signedIn);
    ui->init_storage_button->setEnabled(signedIn);
    ui->create_file_button->setEnabled(signedIn);
    ui->create_folder_button->setEnabled(signedIn);
    ui->back_button->setEnabled(signedIn);
    ui->upload_button->setEnabled(signedIn);

    ui->register_widget->setVisible(false);
    ui->register_line->setVisible(false);

}

void MainWindow::clearData(){
    ui->login_login->setText("");
    ui->login_pass->setText("");
    ui->sign_up_login->setText("");
    ui->sign_up_pass1->setText("");
    ui->sign_up_pass2->setText("");
}



map<string, QByteArray> MainWindow::sendDataToHost(QByteArray data){
    map<string, QByteArray> resultMap = map<string, QByteArray>();
    int type = data.at(0);
    socket = new QTcpSocket(this);
    socket->connectToHost(hostname, 1234);
    qDebug()<<"here";
    QByteArray result = QByteArray();
    if(socket->waitForConnected(3000)){
        qDebug() << "Connected!";
        qDebug() << data <<data.length();
        int timeout=20, waitTime=500;
        socket->write(data);
        socket->waitForBytesWritten(1000);
        socket->waitForReadyRead(waitTime);
        while(socket->bytesAvailable()<1&&timeout-->0){
            socket->waitForReadyRead(waitTime);
        }
        QByteArray code_ = socket->read(1);
        resultMap.insert(pair<string, QByteArray>("code", code_));
        qDebug()<<bytesToInt(code_);
        if(bytesToInt(resultMap.at("code"))==0x00){
            if(type==CMD_SIGN_IN||type==CMD_SIGN_UP){
                int size=TOKEN_SIZE;
                while(socket->bytesAvailable()<size&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                resultMap.insert(pair<string, QByteArray>("token", socket->read(size)));
            }else if(type==CMD_SIGN_OUT||type==CMD_INIT_STORAGE||type==CMD_CREATE_FILE
                     ||type==CMD_CREATE_FOLDER||type==CMD_FILE_INFO||type==CMD_FILE_DELETE
                     ||type==CMD_DELETE_FOLDER||type==CMD_READ_FOLDER||type==CMD_FILE_COPY||type==CMD_FILE_MOVE){
                qDebug()<<1;
                int size=0;
                while(socket->bytesAvailable()<2&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                size = bytesToInt(socket->read(2));
                while(socket->bytesAvailable()<size&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                resultMap.insert(pair<string, QByteArray>("message", socket->read(size)));
            }else if(type==CMD_READ_FILE||type==CMD_UPLOAD_FILE){
                while(socket->bytesAvailable()<1&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                int servType = bytesToInt(socket->read(1));
                qDebug()<<"servType:"<<servType;
                while(socket->bytesAvailable()<2&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                resultMap.insert(pair<string, QByteArray>("port", socket->read(2)));
                QByteArray address;

                int size=4;
                if(servType==2){
                    size=16;
                }
                while(socket->bytesAvailable()<size&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                resultMap.insert(pair<string, QByteArray>("address", socket->read(size)));
                qDebug()<<"address:"<<servType;
                while(socket->bytesAvailable()<16&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                resultMap.insert(pair<string, QByteArray>("token", socket->read(16)));
            }
            if(timeout==0){
                resultMap.at("code")=QByteArray(1, 0x80);
            }
        }else if(type==CMD_READ_FILE||type==CMD_UPLOAD_FILE){

            int size=0;
            while(socket->bytesAvailable()<2&&timeout-->0){
                socket->waitForReadyRead(waitTime);
            }
            size = bytesToInt(socket->read(2));
            while(socket->bytesAvailable()<size&&timeout-->0){
                socket->waitForReadyRead(waitTime);
            }
            resultMap.insert(pair<string, QByteArray>("message", socket->read(size)));
        }
        socket->close();

        return resultMap;
    }
    else
    {
        qDebug()<<"Not connected!";
        return map<string, QByteArray>();
    }

}

bool MainWindow::systemSignOut(){
    username="";
    QByteArray data;
    data.append(CMD_SIGN_OUT);
    data.append(token);
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }

    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        QByteArray message = resultMap.at("message");
        return true;
    }
}

bool MainWindow::systemCreateFile(QString filename){
    QByteArray data;
    data.append(CMD_CREATE_FILE);
    data.append(token);
    QString fullpath = currentFolder+"/"+filename;
    if(currentFolder==""){
        fullpath = filename;
    }
    data.append(fullpath.length());
    data.append(fullpath.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }
    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        if(!filename.contains('/')){
            addFile(filename, false);
        }
        return true;
    }
}

bool MainWindow::systemCreateFolder(QString foldername){
    QByteArray data;
    data.append(CMD_CREATE_FOLDER);
    QString fullname = currentFolder+"/"+foldername;
    if(currentFolder==""){
        fullname=foldername;
    }

    data.append(token);
    data.append(fullname.length());
    data.append(fullname.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }
    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        if(!foldername.contains('/')){
            addFile(foldername, true);
        }

        return true;
    }
}

bool MainWindow::systemInitStorage(){

    QByteArray data;
    data.append(CMD_INIT_STORAGE);
    data.append(token);
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }

    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        QByteArray message = resultMap.at("message");
        return true;
    }
}

bool MainWindow::systemSignUp(QString login, QString password){
    hostname = ui->host_address->text();
    username=login;
    QByteArray data;
    data.append(CMD_SIGN_UP);
    data.append(*extendSizeToN(login.toUtf8(), 20));
    data.append(password.length());
    data.append(password.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }

    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        token = resultMap.at("token");
        return true;
    }
}

bool MainWindow::systemSignIn(QString login, QString password){
    hostname = ui->host_address->text();
    username = login;
    QByteArray data;
    data.append(CMD_SIGN_IN);
    data.append(*extendSizeToN(login.toUtf8(), 20));
    data.append(password.length());
    data.append(password.toUtf8());
    qDebug()<<"here";
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }
    if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        token = resultMap.at("token");
        return true;
    }

}

std::map<std::string, QByteArray> MainWindow::sendDataToStorageServer(
        QString server, int port, std::map<std::string, QByteArray> data, int type){

    map<string, QByteArray> resultMap = map<string, QByteArray>();
    socket = new QTcpSocket(this);
    socket->connectToHost(server, port);

    if(socket->waitForConnected(3000)){
        qDebug() << "Connected!";
        int waitTime=500;
        socket->write(data.at("token"));
        socket->waitForBytesWritten(3000);
        if(type==CMD_UPLOAD_FILE){
            QByteArray file = data.at("file");
            int size = file.size();
            qDebug()<<size;
            int sent=0;
            int percent = 0;
            QMessageBox messageBox;
            QProgressDialog * progress = new QProgressDialog("Uploading file...", "Cancel", 0, 100, this, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
            progress->setWindowModality(Qt::WindowModal);
            progress->show();

            for(int i=0; i<size;i++){
                socket->write(QByteArray(1, file.at(i)));
                socket->waitForBytesWritten(waitTime);
                sent++;
//                if(socket->state()!=socket->ConnectedState){
//                    break;
//                }
                if (progress->wasCanceled()){
                    socket->close();
                    break;
                }
                if(sent%10000==0){
                    progress->setValue((sent*100)/size);
                    QCoreApplication::processEvents();
                }

            }
            progress->cancel();
            if(sent==size){
                messageBox.setText("Upload finished");
                messageBox.exec();
                resultMap.insert(pair<string, QByteArray>("code", QByteArray(1, 0)));
            }else{
                resultMap.insert(pair<string, QByteArray>("code", QByteArray(1, 1)));
            }

            qDebug()<<bytesToInt(resultMap.at("code"));
            socket->close();
        }else if(type==CMD_READ_FILE){
            int timeout=20;
            while(socket->bytesAvailable()<1&&timeout-->0){
                socket->waitForReadyRead(waitTime);
            }
            QByteArray code_ = socket->read(1);
            qDebug()<<"code"<<code_;
            
            QProgressDialog progress("Downloadng file...", "Cancel", 0, 100, this, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
               progress.setWindowModality(Qt::WindowModal);

            progress.show();
            if(bytesToInt(code_)==0){
                while(socket->bytesAvailable()<4&&timeout-->0){
                    socket->waitForReadyRead(waitTime);
                }
                int size = bytesToInt(socket->read(4));
                qDebug()<<"size"<<size;
                QByteArray data = QByteArray();
                while(data.length()<size){
                    if(data.length()%10000==0){
                        progress.setValue(data.length()*100/size);
                        QCoreApplication::processEvents();
                    }
                    qDebug()<<"size__"<<data.length();
                    timeout=20;
                    if (progress.wasCanceled()){
                        socket->close();
                        break;
                    }

                    while(socket->bytesAvailable()==0&&timeout-->0){
                        socket->waitForReadyRead(waitTime);
                    }
//                    if(socket->state()!=socket->ConnectedState){
//                        break;
//                    }
                    
                    data.append(socket->readAll());
                }
                progress.cancel();
                qDebug()<<"size end"<<data.length();
                if(size==data.length()){
                    resultMap.insert(pair<string, QByteArray>("code", QByteArray(1, 0)));
                    resultMap.insert(pair<string, QByteArray>("file", data));
                }else{
                    resultMap.insert(pair<string, QByteArray>("code", QByteArray(1, 1)));
                }
            }else{
                resultMap.insert(pair<string, QByteArray>("code", QByteArray(1, 1)));
            }
        }
    }else{
       qDebug() << "Not connected!";
    }
    return resultMap;
}

bool MainWindow::systemUploadFile(QString filename){
    QByteArray data;
    QFile * file = new QFile(filename);
    QString nameOfFile = filename.split("/").last();
    if(file->exists()){

        qDebug()<<"filesize"<<file->size();
    }else{
        makePopup("File does not exist");
        return false;
    }

    map<string, QByteArray> storageData = map<string, QByteArray>();
    file->open(QIODevice::ReadOnly);
    int filesize = file->size();
    storageData.insert(pair<string, QByteArray>("file", file->readAll()));
    file->close();

    data.append(CMD_UPLOAD_FILE);
    data.append(token);
    QString fullname = currentFolder+"/"+nameOfFile;
    if(currentFolder==""){
        fullname=nameOfFile;
    }
    data.append(fullname.length());

    QByteArray filesizeArr;
    filesizeArr.append((char)(filesize/256/256/256));
    filesizeArr.append((char)(filesize/256/256%256));
    filesizeArr.append((char)(filesize/256%256%256));
    filesizeArr.append((char)(filesize%256%256%256));
    data.append(filesizeArr);
    data.append(fullname.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
        return false;
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        qDebug()<<resultMap.at("code").at(0);
        makePopup(resultMap.at("code").at(0));
        return false;
    }else{
        int port = bytesToInt(resultMap.at("port"));
        QString address;
        QByteArray addr = resultMap.at("address");
        if(addr.size()==4){
            address.append(QString::number((unsigned int)(unsigned char)addr.at(0)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(1)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(2)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(3)));
        }else{
            //TODO IPv6
        }
        QByteArray serverToken = resultMap.at("token");
        qDebug()<<address<<port<<serverToken;
        storageData.insert(pair<string, QByteArray>("token", serverToken));
        map<string, QByteArray> resultMap = sendDataToStorageServer(address, port, storageData, CMD_UPLOAD_FILE);

        return bytesToInt(resultMap.at("code"))==0;
    }
}


QByteArray * MainWindow::extendSizeToN(QByteArray array, int n){
    if(array.length()>=n){
        // error
        return new QByteArray();
    }else{
        QByteArray *result = new QByteArray(n-array.length(), 0x00);
        result->push_front(array);
        return result;
    }
}

void MainWindow::makePopup(QString text){
    qDebug()<<"popup";
    popUp->setPopupText(text);
    popUp->show();
}
void MainWindow::makePopup(unsigned char errorCode){
    makePopup(errorMessages.at((unsigned int)errorCode));
}

void MainWindow::addFile(QString name, bool folderFlag){

    qDebug()<<name;
    QString filename = name;
    QString path_to_file = ":/images/file.png";
    if(folderFlag){
        path_to_file=":/images/folder.png";
    }
    QHBoxLayout *newLayout = new QHBoxLayout();
    QLabel *logo = new QLabel();
    QImage image = QImage(path_to_file);
    logo->setPixmap(QPixmap::fromImage(image));
    logo->setMinimumSize(36, 36);
    logo->setMaximumSize(36, 36);
    logo->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);
    QLabel *label = new QLabel(filename);
    label->setMinimumHeight(36);
    label->setMaximumHeight(36);
    QFont font = label->font();
    font.setPointSize(10);
    label->setFont(font);
    label->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Minimum);

    newLayout->addWidget(logo);
    newLayout->addWidget(label);
    newLayout->setMargin(2);
    qDebug()<<newLayout->count();
    QFrame *line;
    line = new QFrame();
    line->setFrameShape(QFrame::HLine);
    line->setFrameShadow(QFrame::Sunken);
    RWidget * widget = new RWidget();
    widget->filename=filename;
    widget->folderFlag = folderFlag;

    widget->setLayout(newLayout);
    files.push_back(widget);
    connect(widget, SIGNAL(mouseRightClickEvent(RWidget *)),
                SLOT(customMenuRequested(RWidget *)));
    ui->files_layout->insertWidget(0, line);
    ui->files_layout->insertWidget(0, widget);




    ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
}

void MainWindow::customMenuRequested(RWidget * widget){
    /* Создаем объект контекстного меню */
    QMenu * menu = new QMenu(this);
    currentWidget = widget;
    /* Создаём действия для контекстного меню */
    if(widget->folderFlag){
        QAction * openFolder = new QAction(trUtf8("Open"), this);
        QAction * deleteFolder = new QAction(trUtf8("Delete"), this);
        /* Подключаем СЛОТы обработчики для действий контекстного меню */
        connect(openFolder, SIGNAL(triggered()), this, SLOT(openFolder()));
        connect(deleteFolder, SIGNAL(triggered()), this, SLOT(deleteFolder()));
        /* Устанавливаем действия в меню */
        menu->addAction(openFolder);
        menu->addAction(deleteFolder);
    }else{
        QAction * openFile = new QAction(trUtf8("Open"), this);
        QAction * copyFile = new QAction(trUtf8("Copy"), this);
        QAction * moveFile = new QAction(trUtf8("Move"), this);
        QAction * getInfo = new QAction(trUtf8("Get info"), this);
        QAction * deleteFile = new QAction(trUtf8("Delete"), this);
        /* Подключаем СЛОТы обработчики для действий контекстного меню */
        connect(getInfo, SIGNAL(triggered()), this, SLOT(getFileInfo()));
        connect(openFile, SIGNAL(triggered()), this, SLOT(openFile()));
        connect(deleteFile, SIGNAL(triggered()), this, SLOT(deleteFile()));
        connect(copyFile, SIGNAL(triggered()), this, SLOT(copyFile()));
        connect(moveFile, SIGNAL(triggered()), this, SLOT(moveFile()));
        /* Устанавливаем действия в меню */
        menu->addAction(openFile);
        menu->addAction(copyFile);
        menu->addAction(moveFile);
        menu->addAction(getInfo);
        menu->addAction(deleteFile);
    }

    /* Вызываем контекстное меню */
    menu->popup(cursor().pos());
}

void MainWindow::getFileInfo(){
    QByteArray data;
    data.append(CMD_FILE_INFO);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname=currentWidget->filename;
    }

    data.append(fullname.length());
    data.append(fullname.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        QString message = resultMap.at("message");
        QStringList list = message.split(" ");
        QString text = "";
        text.append("Name of file:\t"+currentWidget->filename+"\n");
        text.append("Size of file:\t"+list.at(0)+"\n");
        text.append("Qty of replicas:\t"+list.at(1)+"\n");
        text.append("Creation date:\t"+list.at(2)+"\n");
        text.append("Creation time:\t"+list.at(3)+"\n");
        QMessageBox messageBox;
        messageBox.setText(text);
        messageBox.exec();
    }
}
void MainWindow::deleteFile(){
    QMessageBox::StandardButton reply;
    reply = QMessageBox::question(this, "File delete", "This file will be permanently deleted. Are you sure?",
                               QMessageBox::Yes|QMessageBox::No);
    if (reply != QMessageBox::Yes) {
        return;
    }
    QByteArray data;
    data.append(CMD_FILE_DELETE);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname=currentWidget->filename;
    }
    data.append(fullname.length());
    data.append(fullname.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);

    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        qDebug()<<resultMap.at("message");
        for(int i=0; i<ui->files_layout->count(); i+=2){
            if(((RWidget *)(ui->files_layout->itemAt(i)->widget()))->filename==currentWidget->filename){
                delete ui->files_layout->takeAt(i)->widget();
                delete ui->files_layout->takeAt(i)->widget();
                break;
            }
        }
        currentWidget=0;
    }
}
void MainWindow::copyFile(){
    QByteArray data;
    data.append(CMD_FILE_COPY);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname=currentWidget->filename;
    }
    bool ok;
    QString text = QInputDialog::getText(this, "Copy file to filename",
                                         "Full path:", QLineEdit::Normal, "", &ok, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
    if (ok){
        if(text.isEmpty()){
            makePopup("You should provide full path!");
            return;
        }
    }else{
        return;
    }

    data.append(fullname.length());
    data.append(text.length());
    data.append(fullname.toUtf8());
    data.append(text.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);

    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        qDebug()<<resultMap.at("message");
        QLayoutItem *child;
        while(ui->files_layout->count()>1&&(child=ui->files_layout->takeAt(0))!=0){
            delete child->widget();
        }

        ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
        QByteArray data = QByteArray();
        data.append(CMD_READ_FOLDER);
        data.append(token);
        data.append(currentFolder.length());
        data.append(currentFolder.toUtf8());
        map<string, QByteArray> resultMap = sendDataToHost(data);
        if(resultMap.empty()){
            makePopup("Connecton error!");
        }else if(bytesToInt(resultMap.at("code"))!=0x00){
            makePopup(resultMap.at("code").at(0));
        }else{
            QString message = resultMap.at("message");
            qDebug()<<message;
            QStringList list = message.split("\r\n");
            for(int i=0; i<list.size(); i++){
                QStringList parsedList = list.at(i).split(" ");
                if(parsedList.at(0)=="f"){
                    QString filename = "";
                    for(int i=1; i<parsedList.size(); i++){
                        filename+=parsedList.at(i);
                        if(i<parsedList.size()-1){
                            filename+=" ";
                        }

                    }
                    addFile(filename, false);
                }else if(parsedList.at(0)=="d"){
                    if(!parsedList.at(1).contains("/")){
                        QString filename = "";
                        for(int i=1; i<parsedList.size(); i++){
                            filename+=parsedList.at(i);
                            if(i<parsedList.size()-1){
                                filename+=" ";
                            }
                        }
                        addFile(filename, true);
                    }
                }
            }
        }
    }
}
void MainWindow::moveFile(){
    QByteArray data;
    data.append(CMD_FILE_MOVE);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname=currentWidget->filename;
    }
    bool ok;
    QString text = QInputDialog::getText(this, "Move file to filename",
                                         "Full path:", QLineEdit::Normal, "", &ok, Qt::WindowSystemMenuHint | Qt::WindowTitleHint);
    if (!ok){
        return;
    }

    data.append(fullname.length());
    data.append(text.length());
    data.append(fullname.toUtf8());
    data.append(text.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);

    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        qDebug()<<resultMap.at("message");
        QLayoutItem *child;
        while(ui->files_layout->count()>1&&(child=ui->files_layout->takeAt(0))!=0){
            delete child->widget();
        }

        ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
        QByteArray data = QByteArray();
        data.append(CMD_READ_FOLDER);
        data.append(token);
        data.append(currentFolder.length());
        data.append(currentFolder.toUtf8());
        map<string, QByteArray> resultMap = sendDataToHost(data);
        if(resultMap.empty()){
            makePopup("Connecton error!");
        }else if(bytesToInt(resultMap.at("code"))!=0x00){
            makePopup(resultMap.at("code").at(0));
        }else{
            QString message = resultMap.at("message");
            qDebug()<<message;
            QStringList list = message.split("\r\n");
            for(int i=0; i<list.size(); i++){
                QStringList parsedList = list.at(i).split(" ");
                if(parsedList.at(0)=="f"){
                    QString filename = "";
                    for(int i=1; i<parsedList.size(); i++){
                        filename+=parsedList.at(i);
                        if(i<parsedList.size()-1){
                            filename+=" ";
                        }

                    }
                    addFile(filename, false);
                }else if(parsedList.at(0)=="d"){
                    if(!parsedList.at(1).contains("/")){
                        QString filename = "";
                        for(int i=1; i<parsedList.size(); i++){
                            filename+=parsedList.at(i);
                            if(i<parsedList.size()-1){
                                filename+=" ";
                            }
                        }
                        addFile(filename, true);
                    }
                }
            }
        }
    }
}
void MainWindow::openFile(){
    QByteArray data;
    data.append(CMD_READ_FILE);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname=currentWidget->filename;
    }
    data.append(fullname.length());
    data.append(fullname.toUtf8());
    map<string, QByteArray> resultMap = sendDataToHost(data);
    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        int port = bytesToInt(resultMap.at("port"));
        QString address;
        QByteArray addr = resultMap.at("address");
        if(addr.size()==4){
            address.append(QString::number((unsigned int)(unsigned char)addr.at(0)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(1)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(2)));
            address.append(".");
            address.append(QString::number((unsigned int)(unsigned char)addr.at(3)));
        }else{
            //TODO IPv6
        }
        QByteArray serverToken = resultMap.at("token");
        qDebug()<<address<<port<<serverToken;
        map<string, QByteArray> storageData = map<string, QByteArray>();
        storageData.insert(pair<string, QByteArray>("token", serverToken));
        map<string, QByteArray> resultMap = sendDataToStorageServer(address, port, storageData, CMD_READ_FILE);

        if(bytesToInt(resultMap.at("code"))==0){
            QFile file(QFileDialog::getSaveFileName(NULL, tr("Save file")));
            file.open(QIODevice::WriteOnly);
            file.write(resultMap.at("file"));
            file.close();
        }else{
            makePopup("Connection error");
        }
    }
}

void MainWindow::changeDir(QString nextDir, bool nextFlag, bool readDir){

    qDebug()<<"not skip";
    if(nextFlag){
        if(currentFolder==""){
            currentFolder=nextDir;
        }else{
            currentFolder+="/"+nextDir;
        }
    }else if(currentFolder!=""){
        QStringList list = currentFolder.split("/");
        list.removeLast();
        currentFolder="";
        for(int i=0; i<list.size(); i++){
            if(i>0){
                currentFolder+="/";
            }
            currentFolder+=list.at(i);
        }

    }
    ui->folder_label->setText(currentFolder);
    ui->back_button->setEnabled(currentFolder!="");

    if(readDir){
        QLayoutItem *child;
        while(ui->files_layout->count()>1&&(child=ui->files_layout->takeAt(0))!=0){
            delete child->widget();
        }

        ui->scrollAreaWidgetContents_2->setMinimumHeight(ui->files_layout->minimumSize().height());
        QByteArray data = QByteArray();
        data.append(CMD_READ_FOLDER);
        data.append(token);
        data.append(currentFolder.length());
        data.append(currentFolder.toUtf8());
        map<string, QByteArray> resultMap = sendDataToHost(data);
        if(resultMap.empty()){
            makePopup("Connecton error!");
        }else if(bytesToInt(resultMap.at("code"))!=0x00){
            makePopup(resultMap.at("code").at(0));
        }else{
            QString message = resultMap.at("message");
            qDebug()<<message;
            QStringList list = message.split("\r\n");
            for(int i=0; i<list.size(); i++){
                QStringList parsedList = list.at(i).split(" ");
                if(parsedList.at(0)=="f"){
                    QString filename = "";
                    for(int i=1; i<parsedList.size(); i++){
                        filename+=parsedList.at(i);
                        if(i<parsedList.size()-1){
                            filename+=" ";
                        }

                    }
                    addFile(filename, false);
                }else if(parsedList.at(0)=="d"){
                    if(!parsedList.at(1).contains("/")){
                        QString filename = "";
                        for(int i=1; i<parsedList.size(); i++){
                            filename+=parsedList.at(i);
                            if(i<parsedList.size()-1){
                                filename+=" ";
                            }
                        }
                        addFile(filename, true);
                    }
                }
            }
        }
    }
}

void MainWindow::openFolder(){
    changeDir(currentWidget->filename, true, true);
}
void MainWindow::deleteFolder(){
    QMessageBox::StandardButton reply;
    reply = QMessageBox::question(this, "Folder delete", "This folder will be permanently deleted with all files inside. Are you sure?",
                               QMessageBox::Yes|QMessageBox::No);
    if (reply != QMessageBox::Yes) {
        return;
    }
    QByteArray data;
    data.append(CMD_DELETE_FOLDER);
    data.append(token);
    QString fullname = currentFolder+"/"+currentWidget->filename;
    if(currentFolder==""){
        fullname = currentWidget->filename;
    }
    data.append(fullname.length());
    data.append(fullname.toUtf8());
    qDebug()<<"here1";
    map<string, QByteArray> resultMap = sendDataToHost(data);
    qDebug()<<"here2";
    if(resultMap.empty()){
        makePopup("Connecton error!");
    }else if(bytesToInt(resultMap.at("code"))!=0x00){
        makePopup(resultMap.at("code").at(0));
    }else{
        qDebug()<<resultMap.at("message");
        for(int i=0; i<ui->files_layout->count(); i+=2){
            if(((RWidget *)(ui->files_layout->itemAt(i)->widget()))->filename==currentWidget->filename){
                delete ui->files_layout->takeAt(i)->widget();
                delete ui->files_layout->takeAt(i)->widget();
                break;
            }
        }
        currentWidget=0;
    }
}

int MainWindow::bytesToInt(QByteArray array){
    qDebug()<<array.toHex();
    if(array.size()>4){
        return -1;
    }else{

        int result = 0;
        for(int i=0; i<array.size(); i++){
            result*=256;
            result+=(unsigned char)array.at(i);
            qDebug()<<"pos:"<<i<<"data:"<<(unsigned int)(unsigned char)array.at(i);
            qDebug()<<"res"<<result;
        }
        return result;
    }
}


MainWindow::~MainWindow()
{
    delete ui;
}

