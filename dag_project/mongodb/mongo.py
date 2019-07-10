from mongoengine import *
import datetime


class textHistory(Document):
    url = StringField()
    id = StringField()
    text = ListField()
    isurl = StringField()
    c_date = DateTimeField()
    m_date = DateTimeField(default=datetime.datetime.now())
    meta = {
        'indexes': [
            'id',
            'url',
            'c_date'
            # ,{
            #     'fields': ['page_id', 'modified_date'],
            #     'unique': True
            # }
        ]
    }

    def save(self, *args, **kwargs):
        if not self.c_date:
            self.c_date = datetime.datetime.now()
        self.m_date = datetime.datetime.now()
        return super(textHistory, self).save(*args, **kwargs)

    def __unicode__(self):
        return self.url

    def __repr__(self):
        return '<textHistory {uk.url}>'.format(uk=self)

    def to_dict(self):
        return {
            "url": self.url,
            "text": self.keywords,
            "isurl": self.isurl,
            "id": self.page_id,
            "c_date": self.c_date
        }
