# coding: utf-8
import lxml.etree


def doIt():
    students = '''
    <students>
        <student x="1">
            <id>001</id>
            <name>Kim</name>
        </student>
        <student x="2">
            <id>002</id>
            <name>Lee</name>
        </student>
    </students>
    '''
    root = lxml.etree.fromstring(students)
    total = 0
    for student in root.findall('student'):
        total += int(student.find('id').text)
    print(total)


if __name__ == '__main__':
    doIt()
