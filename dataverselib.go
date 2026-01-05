package dataverselib

// Exported function
func NewUser(id int, name, email string) User {
	return User{
		ID:    id,
		Name:  name,
		Email: email,
	}
}
