package Queue

import (
	"errors"
	"fmt"
	"sort"
)

type customer struct {
	wid      string
	priority int
}

func New(wid string, priority int) customer {
	return customer{
		wid:      wid,
		priority: priority,
	}
}

type ByPriority []customer

func (a ByPriority) Len() int           { return len(a) }
func (a ByPriority) Less(i, j int) bool { return a[i].priority < a[j].priority }
func (a ByPriority) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (q *Queue) SortCustomers() {
	sort.Sort(ByPriority(q.customers))
}

type Queue2 struct {
	items []string
}

func (q *Queue2) Enqueue2(item string) {
	q.items = append(q.items, item)
}

func (q *Queue2) SearchAndRemove(item string) bool {
	for i, v := range q.items {
		if v == item {
			// Remove the item from the slice
			q.items = append(q.items[:i], q.items[i+1:]...)
			return true
		}
	}
	return false
}

type Queue struct {
	Size      int
	customers []customer
}

func (q *Queue) Enqueue(c customer) (bool, error) {
	if q.Size > 0 && len(q.customers) >= q.Size {
		return true, errors.New("Queue is full") // Queue is full
	}
	q.customers = append(q.customers, c)
	return false, nil
}

func (q *Queue) Dequeue() (string, error) {
	if len(q.customers) == 0 {
		return " ", errors.New("Queue is empty")
	}
	c := q.customers[0]
	q.customers = q.customers[1:]
	return c.wid, nil
}

func (q *Queue) GetLength() int {
	return len(q.customers)
}

func (q *Queue) Display() {
	if len(q.customers) == 0 {
		fmt.Println("Queue is empty")
		return
	}
	fmt.Println("Customers in the Queue:")
	for _, c := range q.customers {
		fmt.Printf("WID: %s, Priority: %d\n", c.wid, c.priority)
	}
}

func (q *Queue) IsEmpty() bool {
	return len(q.customers) == 0
}
