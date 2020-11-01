#include "rwidget.h"

RWidget::RWidget(QWidget *parent) : QWidget(parent)
{

}
void RWidget::mousePressEvent(QMouseEvent *mouseEvent)
{
    if (mouseEvent->button() == Qt::RightButton) {
        this->mouseRightClickEvent(this);
    }
}
