package model

type TreeElem struct {
	TreeElemId    *uint   `json:"treeElemId"`
	HierarchyId   *uint   `json:"hierarchyId"`
	BranchLevel   *uint   `json:"branchLevel"`
	SlotNumber    *uint   `json:"slotNumber"`
	Name          *string `json:"name"`
	ContainerType *uint16 `json:"containerType"`
	Description   *string `json:"description"`
	ElementEnable *uint16 `json:"elementEnable"`
	ParentEnable  *uint   `json:"parentEnable"`
	HierarchyType *uint   `json:"hierarchyType"`
	AlarmFlags    *uint16 `json:"alarmFlags"`
	ParentId      *uint   `json:"parentId"`
	ParentRefId   *uint   `json:"parentRefId"`
	ReferenceId   *uint   `json:"referenceId"`
	Good          *uint   `json:"good"`
	Alert         *uint   `json:"alert"`
	Danger        *uint   `json:"danger"`
	Overdue       *uint   `json:"overdue"`
	ChannelEnable *uint   `json:"channelEnable"`
}
