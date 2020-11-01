#ifndef RWIDGET_H
#define RWIDGET_H

#include <QWidget>
#include <QMouseEvent>

class RWidget : public QWidget
{
    Q_OBJECT
public:
    explicit RWidget(QWidget *parent = nullptr);
    QString filename;
    bool folderFlag;
protected:
    void mousePressEvent(QMouseEvent *event) override;
signals:
    void mouseRightClickEvent(RWidget * widget);

};

#endif // RWIDGET_H
